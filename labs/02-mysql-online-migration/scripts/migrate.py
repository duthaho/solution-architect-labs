"""Online schema migration orchestrator (gh-ost-style).

  1. PREFLIGHT   sanity-check binlog config, MySQL version, no triggers/FKs.
  2. GHOST       create _orders_gst LIKE orders, ALTER it to the new schema
                 (instant — the table is empty).
  3. STREAM      record the binlog position, start the applier: every
                 INSERT/UPDATE/DELETE on orders is replayed onto the ghost.
  4. BACKFILL    copy existing rows in small chunks:
                 INSERT IGNORE ... SELECT ... FOR SHARE, chunked by PK,
                 throttled, and paused whenever applier lag grows.
  5. CONVERGE    wait until heartbeat lag through the binlog pipeline is tiny.
  6. CUTOVER     (inside the applier thread — it must own the locks)
                 LOCK both tables -> marker -> drain -> atomic RENAME -> unlock.
                 App writes queue for ~1s; nothing errors, nothing is lost.
  7. The old table stays parked as _orders_old for rollback.

Why the backfill and the stream can run concurrently without coordination:
backfill uses INSERT IGNORE (never overwrites), the applier uses REPLACE
(always overwrites). For any row, binlog data is newer than or equal to
backfill data, so "binlog wins" is exactly right. The one race this doesn't
cover — DELETE applied to the ghost before the backfill re-inserts the stale
row — is closed by FOR SHARE on the chunk SELECT; see README §3.4.
"""
import argparse
import json
import time

import applier as applier_mod
from common import (
    DB,
    GHOST_TABLE,
    LAB_DIR,
    OLD_TABLE,
    STATE_FILE,
    TABLE,
    binlog_position,
    connect,
    log,
    table_columns,
    table_exists,
    wait_for_mysql,
)

CHUNK = 2000
CONVERGE_LAG_MS = 1500
CONVERGE_TIMEOUT_S = 300
BACKPRESSURE_LAG_MS = 3000  # pause backfill while the applier is this far behind


def preflight(conn) -> None:
    checks = {
        "log_bin": "ON",
        "binlog_format": "ROW",
        "binlog_row_image": "FULL",
        "binlog_row_metadata": "FULL",
    }
    with conn.cursor() as cur:
        for var, expected in checks.items():
            cur.execute(f"SHOW VARIABLES LIKE '{var}'")
            actual = cur.fetchone()[1]
            if actual.upper() != expected:
                raise SystemExit(f"Preflight failed: {var}={actual}, need {expected}")

        cur.execute("SELECT VERSION()")
        version = cur.fetchone()[0]
        major, minor, patch = (int(x) for x in version.split("-")[0].split(".")[:3])
        if (major, minor, patch) < (8, 0, 13):
            raise SystemExit(
                f"MySQL {version} < 8.0.13: RENAME under LOCK TABLES unavailable — "
                "you need gh-ost's two-connection cutover dance (see README §3.5)")

        # gh-ost refuses these too: triggers fire on the wrong table after
        # rename; FK children would still point at the old table.
        cur.execute(
            "SELECT trigger_name FROM information_schema.triggers "
            "WHERE event_object_schema=%s AND event_object_table=%s", (DB, TABLE))
        if cur.rowcount:
            raise SystemExit(f"Preflight failed: {TABLE} has triggers")
        cur.execute(
            "SELECT constraint_name FROM information_schema.referential_constraints "
            "WHERE constraint_schema=%s AND (table_name=%s OR referenced_table_name=%s)",
            (DB, TABLE, TABLE))
        if cur.rowcount:
            raise SystemExit(f"Preflight failed: {TABLE} participates in foreign keys")

    for t in (GHOST_TABLE, OLD_TABLE):
        if table_exists(conn, t):
            raise SystemExit(
                f"Preflight failed: {t} already exists "
                f"(previous run? `DROP TABLE {t}` after inspecting it)")
    log.info("Preflight OK (binlog ROW/FULL/FULL, MySQL %s, no triggers/FKs)", version)


def backfill(conn, applier, shared_cols: list[str], sleep_ms: int) -> int:
    """Chunked copy of all rows that existed when the stream started.

    Chunk boundaries come from a PK-order probe query (gh-ost does the same):
    range arithmetic on id would produce empty or oversized chunks wherever
    deletes have left gaps. Rows inserted after `max_id` are the stream's job.
    """
    cols = ", ".join(f"`{c}`" for c in shared_cols)
    with conn.cursor() as cur:
        cur.execute(f"SELECT MIN(id), MAX(id) FROM {TABLE}")
        min_id, max_id = cur.fetchone()
        if min_id is None:
            return 0

        copied = 0
        last = min_id - 1
        t0 = time.time()
        t_log = t0
        while last < max_id:
            cur.execute(
                f"SELECT id FROM {TABLE} WHERE id > %s ORDER BY id LIMIT %s, 1",
                (last, CHUNK - 1))
            row = cur.fetchone()
            upper = row[0] if row else max_id
            cur.execute(
                f"INSERT IGNORE INTO {GHOST_TABLE} ({cols}) "
                f"SELECT {cols} FROM {TABLE} "
                f"WHERE id > %s AND id <= %s FOR SHARE",
                (last, upper))
            copied += cur.rowcount
            last = upper

            # Backpressure: a backfill that outruns the applier just moves the
            # wait to the cutover. Yield until the stream catches up.
            while applier.lag_ms > BACKPRESSURE_LAG_MS and applier.error is None:
                time.sleep(0.2)
            if applier.error:
                raise SystemExit(f"Applier died during backfill: {applier.error}")
            if sleep_ms:
                time.sleep(sleep_ms / 1000)
            if time.time() - t_log > 5:
                rate = copied / (time.time() - t0)
                log.info("  backfill: %d rows (%.0f rows/s, applier lag %.0fms)",
                         copied, rate, applier.lag_ms)
                t_log = time.time()
        return copied


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--chunk-sleep-ms", type=int, default=0,
                        help="throttle: sleep between backfill chunks (production: start at 50)")
    args = parser.parse_args()

    wait_for_mysql()
    conn = connect()

    log.info("STEP 1: preflight")
    preflight(conn)

    # -- Step 2: ghost table --------------------------------------------------
    log.info("STEP 2: create %s with the target schema", GHOST_TABLE)
    with conn.cursor() as cur:
        cur.execute(f"CREATE TABLE {GHOST_TABLE} LIKE {TABLE}")
        cur.execute((LAB_DIR / "sql" / "v2_alter.sql").read_text())
    log.info("Ghost table ready (ALTER on an empty table: instant)")

    # -- Step 3: start streaming ----------------------------------------------
    # Position is captured BEFORE the backfill reads anything: every commit
    # after this point reaches the ghost via the stream, every commit before
    # it is present in the table for the backfill to copy. The overlap is
    # harmless by design (REPLACE/INSERT IGNORE idempotency).
    start_file, start_pos = binlog_position(conn)
    log.info("STEP 3: stream binlog from %s:%d -> apply onto %s", start_file, start_pos, GHOST_TABLE)
    applier = applier_mod.BinlogApplier(
        source_table=TABLE, target_table=GHOST_TABLE,
        start_file=start_file, start_pos=start_pos,
        renames=[(TABLE, OLD_TABLE), (GHOST_TABLE, TABLE)],
    )
    heartbeat = applier_mod.Heartbeat()
    applier.start()
    heartbeat.start()

    # -- Step 4: backfill -----------------------------------------------------
    log.info("STEP 4: chunked backfill (chunk=%d, INSERT IGNORE + FOR SHARE)", CHUNK)
    t0 = time.time()
    copied = backfill(conn, applier, applier.shared_cols, args.chunk_sleep_ms)
    log.info("Backfill done: %d rows in %.1fs (stream applied %d events meanwhile)",
             copied, time.time() - t0, applier.events_applied)

    # -- Step 5: converge -----------------------------------------------------
    # Same lesson as lab 01: convergence is a TIME bound, not a count. Lag
    # below ~1.5s means the final locked drain will finish in about that long,
    # which is what actually bounds the cutover window.
    log.info("STEP 5: converge (heartbeat lag through the binlog pipeline < %dms)", CONVERGE_LAG_MS)
    deadline = time.time() + CONVERGE_TIMEOUT_S
    while applier.lag_ms > CONVERGE_LAG_MS:
        if applier.error:
            raise SystemExit(f"Applier died: {applier.error}")
        if time.time() > deadline:
            raise SystemExit(
                "Never converged: write rate exceeds apply rate. Throttle the app "
                "or batch the applier, then retry.")
        time.sleep(0.2)
    log.info("Converged: applier lag %.0fms, %d events applied", applier.lag_ms, applier.events_applied)

    # -- Step 6: cutover ------------------------------------------------------
    log.info("STEP 6: CUTOVER — lock, drain to marker, atomic rename")
    applier.request_cutover()
    applier.join(timeout=60)
    heartbeat.stop()
    if applier.error:
        raise SystemExit(f"Cutover failed: {applier.error}")
    if not applier.cutover_done:
        raise SystemExit("Cutover timed out — applier never saw the marker")

    post_file, post_pos = applier.post_cutover_position
    STATE_FILE.write_text(json.dumps({
        "cutover_ts_ms": int(time.time() * 1000),
        "cutover_binlog_file": post_file,
        "cutover_binlog_pos": post_pos,
        "old_table": OLD_TABLE,
    }, indent=2))

    log.info("Cutover complete: write-block window %.2fs (app writes queued, none failed)",
             applier.block_window_s)
    log.info("%s now has the new schema; old table parked as %s (rollback target)", TABLE, OLD_TABLE)
    log.info("Post-cutover binlog position %s:%d saved to %s", post_file, post_pos, STATE_FILE.name)
    log.info("Next: python scripts/verify.py")


if __name__ == "__main__":
    main()
