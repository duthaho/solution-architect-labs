"""Benchmark the three strategies on identical, freshly re-seeded families.

This RESETS the lab world (bootstrap + seed + journals wiped), then per family:
  * starts that family's live traffic
  * runs the same deterministic delete workload (60 users + 200 orders,
    same ids everywhere — the families are seeded identically) through the
    strategy's own code path, timing each delete as the app would feel it
  * stops traffic; for C, additionally drains the archiver (timed — that cost
    is real, it just isn't on the user's critical path)
  * measures read p95 with each strategy's CORRECT list query
  * measures where the bytes ended up (ANALYZE + information_schema)

Then prints the comparison table that is the point of this whole lab.
"""
import random
import signal
import subprocess
import sys
import time
from pathlib import Path

import archiver
import bootstrap
import seed as seed_mod
import strategy_a
import strategy_b
from common import (FAMILIES, SEED_ROWS, connect, count, journal_path, log,
                    outcome_path, percentiles, table_bytes)

SCRIPTS = Path(__file__).resolve().parent
N_USER_DELETES = 60
N_ORDER_DELETES = 200

DELETE_FNS = {
    "a": (strategy_a.soft_delete_user, strategy_a.soft_delete_order),
    "b": (strategy_b.move_user, strategy_b.move_order),
    "c": (archiver.flag_user, archiver.flag_order),
}
READ_FILTER = {"a": "AND deleted_at IS NULL", "b": "", "c": "AND deleted_at IS NULL"}


def run_workload(family: str) -> dict:
    live = FAMILIES[family]["live"]
    other = FAMILIES[family].get("deleted") or FAMILIES[family].get("archive")
    conn = connect(autocommit=False)
    res: dict = {}

    traffic = subprocess.Popen(
        [sys.executable, str(SCRIPTS / "traffic.py"), family],
        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    time.sleep(2)

    del_user, del_order = DELETE_FNS[family]
    rng = random.Random(42)
    user_ids = rng.sample(range(100, 2000), N_USER_DELETES)
    order_ids = rng.sample(range(1000, SEED_ROWS), N_ORDER_DELETES)
    lat = []
    for uid in user_ids:
        t0 = time.perf_counter()
        del_user(conn, uid)
        lat.append((time.perf_counter() - t0) * 1000)
    for oid in order_ids:
        t0 = time.perf_counter()
        del_order(conn, oid)
        lat.append((time.perf_counter() - t0) * 1000)
    res["del_p50"], res["del_p95"] = percentiles(lat)

    traffic.send_signal(signal.SIGTERM)
    traffic.wait()

    if family == "c":
        t0 = time.perf_counter()
        archiver.mode_run(drain=True)
        res["drain_s"] = time.perf_counter() - t0

    with conn.cursor() as cur:  # read benchmark, correct query per strategy
        cur.execute(f"SELECT MAX(id) FROM {live}.users")
        max_uid = cur.fetchone()[0]
        rlat = []
        for _ in range(300):
            uid = rng.randrange(1, max_uid + 1)
            t0 = time.perf_counter()
            cur.execute(f"SELECT id, status, amount FROM {live}.orders "
                        f"WHERE user_id=%s {READ_FILTER[family]} "
                        f"ORDER BY id DESC LIMIT 10", (uid,))
            cur.fetchall()
            rlat.append((time.perf_counter() - t0) * 1000)
        res["read_p50"], res["read_p95"] = percentiles(rlat)
        for t in ("users", "orders", "order_items"):
            cur.execute(f"ANALYZE TABLE {live}.{t}")
            cur.fetchall()
    conn.commit()

    live_b = sum(sum(table_bytes(conn, live, t)) for t in
                 ("users", "orders", "order_items"))
    other_b = sum(sum(table_bytes(conn, other, t)) for t in
                  ("users", "orders", "order_items")) if other else 0
    res["live_mb"], res["other_mb"] = live_b / 1e6, other_b / 1e6
    res["dead_in_live"] = (count(conn, live, "orders", "deleted_at IS NOT NULL")
                           if family != "b" else 0)
    conn.close()
    return res


def main() -> None:
    for family in FAMILIES:
        journal_path(family).unlink(missing_ok=True)
        outcome_path(family).unlink(missing_ok=True)
    log.info("resetting world: bootstrap + seed (SEED_ROWS=%d)", SEED_ROWS)
    bootstrap.main()
    for family, cfg in FAMILIES.items():
        seed_mod.seed_family(family, cfg["live"])

    results = {f: run_workload(f) for f in FAMILIES}

    rows = [
        ("delete p50 (ms, user-facing)", "{del_p50:.1f}"),
        ("delete p95 (ms, user-facing)", "{del_p95:.1f}"),
        ("read p50 (ms, correct query)", "{read_p50:.2f}"),
        ("read p95 (ms, correct query)", "{read_p95:.2f}"),
        ("live schema size (MB)", "{live_mb:.1f}"),
        ("deleted/archive size (MB)", "{other_mb:.1f}"),
        ("dead rows left in live table", "{dead_in_live}"),
    ]
    print()
    print(f"{'':38s}  {'A deleted_at':>14s}  {'B mirror':>14s}  {'C archiver':>14s}")
    print("-" * 88)
    for label, fmt in rows:
        cells = [fmt.format(**results[f]) for f in FAMILIES]
        print(f"{label:38s}  {cells[0]:>14s}  {cells[1]:>14s}  {cells[2]:>14s}")
    drain = results["c"].get("drain_s")
    print(f"{'archiver drain (s, background)':38s}  {'-':>14s}  {'-':>14s}  "
          f"{drain:>14.1f}")
    print(f"{'restore (undelete) support':38s}  {'flip the flag':>14s}  "
          f"{'reverse move':>14s}  {'copy back':>14s}")
    print("-" * 88)
    log.info("bench done")


if __name__ == "__main__":
    main()
