"""Drill C1 — kill -9 the archiver mid-batch, prove nothing is lost or doubled.

Run WITHOUT traffic on family c (totals must be stable to assert on them):
  1. flag ~10% of orders (+ their items) so the archiver has real work
  2. snapshot per-table totals: live + archive
  3. start the archiver, let it move a few batches, SIGKILL it mid-flight
  4. assert crash-consistency: no id exists in both live and archive,
     totals unchanged  (the batch transaction either committed or vanished)
  5. restart with --drain, assert: zero flagged rows left, totals unchanged
"""
import os
import signal
import subprocess
import sys
import time
from pathlib import Path

from common import connect, count, log

LIVE = "lab10_c"
ARCHIVE = "lab10_c_archive"
TABLES = ["users", "orders", "order_items"]
SCRIPTS = Path(__file__).resolve().parent


def totals(conn) -> dict[str, int]:
    return {t: count(conn, LIVE, t) + count(conn, ARCHIVE, t) for t in TABLES}


def overlap(conn, table: str) -> int:
    with conn.cursor() as cur:
        cur.execute(f"SELECT COUNT(*) FROM {LIVE}.`{table}` l "
                    f"JOIN {ARCHIVE}.`{table}` a ON a.id=l.id")
        return cur.fetchone()[0]


def main() -> None:
    # autocommit: every assertion below must see the archiver's latest commits,
    # not a REPEATABLE READ snapshot from before it started.
    conn = connect()

    with conn.cursor() as cur:  # 1. make work: flag 2000 not-yet-flagged orders
        cur.execute(f"SELECT id FROM {LIVE}.orders WHERE deleted_at IS NULL "
                    f"ORDER BY id LIMIT 2000")
        ids = [str(r[0]) for r in cur.fetchall()]
        assert ids, "family c has no live orders left — re-run make seed"
        id_list = "(" + ",".join(ids) + ")"
        cur.execute(f"UPDATE {LIVE}.order_items SET deleted_at=NOW(6) "
                    f"WHERE order_id IN {id_list} AND deleted_at IS NULL")
        cur.execute(f"UPDATE {LIVE}.orders SET deleted_at=NOW(6) "
                    f"WHERE id IN {id_list}")
    flagged = count(conn, LIVE, "orders", "deleted_at IS NOT NULL")
    assert flagged >= 2000, f"expected >=2000 flagged orders, got {flagged}"
    before = totals(conn)  # 2. snapshot
    log.info("work created: %d flagged orders; totals before: %s", flagged, before)

    # 3. run, then murder. Pacing matters: the kill must land while work
    # remains, or every assertion below passes vacuously — asserted after.
    env = dict(os.environ, BATCH="200", SLEEP_MS="100")
    proc = subprocess.Popen([sys.executable, str(SCRIPTS / "archiver.py"), "run"],
                            env=env)
    time.sleep(1.0)
    proc.send_signal(signal.SIGKILL)
    proc.wait()
    log.info("archiver killed with SIGKILL after 1.0s (pid %d)", proc.pid)

    remaining_mid = count(conn, LIVE, "orders", "deleted_at IS NOT NULL")
    assert remaining_mid > 0, (
        "archiver drained everything before the kill — the crash was never "
        "tested; increase the flagged work or kill sooner")
    for t in TABLES:  # 4. crash-consistency
        assert overlap(conn, t) == 0, f"{t}: row exists in BOTH live and archive"
    after_kill = totals(conn)
    assert after_kill == before, f"totals changed across the crash: {before} -> {after_kill}"
    log.info("post-crash: killed with %d orders still flagged, no live/archive "
             "overlap, totals intact %s", remaining_mid, after_kill)

    subprocess.run([sys.executable, str(SCRIPTS / "archiver.py"), "run", "--drain"],
                   env=env, check=True)  # 5. resume

    for t in TABLES:
        assert overlap(conn, t) == 0, f"{t}: overlap after drain"
    remaining = count(conn, LIVE, "orders", "deleted_at IS NOT NULL")
    after = totals(conn)
    assert remaining == 0, f"{remaining} flagged orders never archived"
    assert after == before, f"totals changed after drain: {before} -> {after}"
    log.info("after restart+drain: 0 flagged rows, totals intact %s", after)
    log.info("drill C1 PASSED — copy+delete in one transaction needs no checkpoint: "
             "the predicate is the work queue")
    conn.close()


if __name__ == "__main__":
    main()
