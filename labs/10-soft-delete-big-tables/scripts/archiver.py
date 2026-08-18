"""Strategy C — soft delete for the app, background archiver for the table.

The app only ever does the instant, safe thing: `UPDATE ... SET
deleted_at=NOW(6)` (mode `flag`). A background archiver (mode `run`) then
moves flagged rows into lab10_c_archive in small batches, each batch one
short transaction:

    SELECT ids FOR UPDATE  ->  INSERT into archive  ->  DELETE from live  ->  COMMIT

Crash-safety needs no checkpoint table: the predicate (deleted_at IS NOT
NULL) *is* the work queue. Copy and delete commit atomically, so kill -9 at
any instant either lands the whole batch or none of it; a restart simply
re-selects whatever is still flagged. The archive PK equals the source PK and
the copy is ON DUPLICATE KEY UPDATE, so even a logic bug that re-copies a row
cannot duplicate it.

Tables drain child-first (order_items -> orders -> users) and parents carry a
"no live children" guard, so FKs never break even while traffic keeps
flagging new families mid-drain.

Modes:
    flag            consume pending delete_requests (instant soft delete)
    run [--drain]   archive batches forever, or until drained with --drain
    purge           delete archive rows past RETENTION_S (records outcomes)
"""
import os
import sys
import time

from common import (connect, count, journal_path, log, outcome_path,
                    append_jsonl, percentiles, read_jsonl)

LIVE = "lab10_c"
ARCHIVE = "lab10_c_archive"
FAMILY = "c"

BATCH = int(os.environ.get("BATCH", "500"))
SLEEP_MS = int(os.environ.get("SLEEP_MS", "100"))
RETENTION_S = int(os.environ.get("RETENTION_S", "3600"))

# (table, live columns, guard: only archive when no live children remain)
SPECS = [
    ("order_items", "id, order_id, sku, qty, price", ""),
    ("orders", "id, user_id, status, amount, created_at",
     f"AND NOT EXISTS (SELECT 1 FROM {LIVE}.order_items i WHERE i.order_id=t.id)"),
    ("users", "id, email, name, created_at",
     f"AND NOT EXISTS (SELECT 1 FROM {LIVE}.orders o WHERE o.user_id=t.id)"),
]


def _in(ids: list[int]) -> str:
    return "(" + ",".join(str(i) for i in ids) + ")"


# ------------------------------------------------------------------ flag mode

def flag_user(conn, user_id: int) -> None:
    with conn.cursor() as cur:
        cur.execute(
            f"UPDATE {LIVE}.order_items oi JOIN {LIVE}.orders o ON oi.order_id=o.id "
            f"SET oi.deleted_at=NOW(6) WHERE o.user_id=%s AND oi.deleted_at IS NULL",
            (user_id,))
        cur.execute(f"UPDATE {LIVE}.orders SET deleted_at=NOW(6) "
                    f"WHERE user_id=%s AND deleted_at IS NULL", (user_id,))
        cur.execute(f"UPDATE {LIVE}.users SET deleted_at=NOW(6) "
                    f"WHERE id=%s AND deleted_at IS NULL", (user_id,))
    conn.commit()


def flag_order(conn, order_id: int) -> None:
    with conn.cursor() as cur:
        cur.execute(f"UPDATE {LIVE}.order_items SET deleted_at=NOW(6) "
                    f"WHERE order_id=%s AND deleted_at IS NULL", (order_id,))
        cur.execute(f"UPDATE {LIVE}.orders SET deleted_at=NOW(6) "
                    f"WHERE id=%s AND deleted_at IS NULL", (order_id,))
    conn.commit()


def mode_flag() -> None:
    conn = connect(autocommit=False)
    done = {(r["table"], r["id"]) for r in read_jsonl(outcome_path(FAMILY))}
    pending = [r for r in read_jsonl(journal_path(FAMILY))
               if r["op"] == "delete_request" and (r["table"], r["id"]) not in done]
    lat = []
    for req in pending:
        t0 = time.perf_counter()
        (flag_user if req["table"] == "users" else flag_order)(conn, req["id"])
        lat.append((time.perf_counter() - t0) * 1000)
        append_jsonl(outcome_path(FAMILY),
                     {"id": req["id"], "table": req["table"], "action": "soft_deleted"})
    p50, p95 = percentiles(lat)
    log.info("flagged %d delete_requests  p50=%.1fms p95=%.1fms — the user waited "
             "for THIS, not for the archiver", len(pending), p50, p95)
    conn.close()


# ------------------------------------------------------------------- run mode

def archive_batch(conn, table: str, cols: str, guard: str) -> int:
    """One short transaction. Returns rows moved."""
    with conn.cursor() as cur:
        cur.execute(f"SELECT t.id FROM {LIVE}.`{table}` t "
                    f"WHERE t.deleted_at IS NOT NULL {guard} "
                    f"ORDER BY t.id LIMIT {BATCH} FOR UPDATE")
        ids = [r[0] for r in cur.fetchall()]
        if not ids:
            conn.rollback()
            return 0
        col_list = cols + ", deleted_at"
        cur.execute(f"INSERT INTO {ARCHIVE}.`{table}` ({col_list}) "
                    f"SELECT {col_list} FROM {LIVE}.`{table}` WHERE id IN {_in(ids)} "
                    f"ON DUPLICATE KEY UPDATE deleted_at=VALUES(deleted_at)")
        cur.execute(f"DELETE FROM {LIVE}.`{table}` WHERE id IN {_in(ids)}")
    conn.commit()
    return len(ids)


def mode_run(drain: bool) -> None:
    conn = connect(autocommit=False)
    moved_total = 0
    log.info("archiver started (batch=%d, sleep=%dms, drain=%s)", BATCH, SLEEP_MS, drain)
    while True:
        moved = 0
        for table, cols, guard in SPECS:
            moved += archive_batch(conn, table, cols, guard)
        moved_total += moved
        if moved == 0:
            if drain:
                break
            time.sleep(1.0)
        else:
            log.info("archived %d rows (total %d)", moved, moved_total)
            time.sleep(SLEEP_MS / 1000)
    log.info("archiver drained: %d rows moved in total", moved_total)
    conn.close()


# ------------------------------------------------------------------ purge mode

def mode_purge() -> None:
    conn = connect(autocommit=False)
    cutoff_sql = f"_archived_at < NOW(6) - INTERVAL {RETENTION_S} SECOND"
    purged = 0
    with conn.cursor() as cur:
        for table in ("order_items", "orders", "users"):
            cur.execute(f"SELECT id FROM {ARCHIVE}.`{table}` WHERE {cutoff_sql}")
            ids = [r[0] for r in cur.fetchall()]
            if ids:
                cur.execute(f"DELETE FROM {ARCHIVE}.`{table}` WHERE id IN {_in(ids)}")
                conn.commit()
                purged += len(ids)
                if table in ("users", "orders"):
                    for i in ids:
                        append_jsonl(outcome_path(FAMILY),
                                     {"id": i, "table": table, "action": "purged"})
    log.info("purged %d archive rows older than %ds — this line is the actual "
             "compliance deadline, not the soft delete", purged, RETENTION_S)
    conn.close()


def main() -> None:
    args = sys.argv[1:]
    mode = args[0] if args else "run"
    if mode == "flag":
        mode_flag()
    elif mode == "run":
        mode_run(drain="--drain" in args)
    elif mode == "purge":
        mode_purge()
    else:
        sys.exit(f"usage: archiver.py flag|run [--drain]|purge (got {mode!r})")


if __name__ == "__main__":
    main()
