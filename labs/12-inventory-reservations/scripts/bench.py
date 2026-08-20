"""Benchmark the three correct strategies under identical hot-item contention.

Each strategy gets a fresh reseed and the same workload: WORKERS x
BENCH_ROUNDS attempts with capacity == demand, so every attempt can succeed
and we measure pure reservation throughput under contention (rejects would
be artificially cheap). Strategy a serializes on one row lock; b holds it
for a single statement; c spreads contention across the slot pool with
SKIP LOCKED — that difference is the whole point of the table.

Exit 1 if any strategy oversells or any drill exits non-zero.
"""

import os
import subprocess
import sys
import time

import common

BENCH_ROUNDS = int(os.environ.get("BENCH_ROUNDS", "40"))
DEMAND = common.WORKERS * BENCH_ROUNDS


def run(mode: str) -> dict:
    env = dict(
        os.environ,
        MODE=mode,
        ROUNDS=str(BENCH_ROUNDS),
        CAPACITY=str(DEMAND),
    )
    subprocess.run(
        [sys.executable, "scripts/seed.py"], env=env, check=True,
        cwd=common.LAB_DIR, capture_output=True,
    )
    t0 = time.monotonic()
    proc = subprocess.run(
        [sys.executable, "scripts/drill_burst.py"], env=env,
        cwd=common.LAB_DIR, capture_output=True,
    )
    wall = time.monotonic() - t0
    if proc.returncode != 0:
        common.log.error("MODE=%s drill failed:\n%s", mode, proc.stderr.decode())
        return {"mode": mode, "failed": True}

    recs = common.read_jsonl(common.journal_path(mode))
    lat = [r["ms"] for r in recs]
    p50, p95 = common.percentiles(lat)
    conn = common.connect()
    with conn.cursor() as cur:
        cur.execute(
            "SELECT COUNT(*) FROM reservations WHERE state IN ('active','committed')"
        )
        consumed = cur.fetchone()[0]
    conn.close()
    return {
        "mode": mode, "failed": False,
        "ops_s": len(recs) / wall,
        "p50": p50, "p95": p95,
        "retries": sum(r["retries"] for r in recs),
        "rejects": sum(1 for r in recs if not r["ok"]),
        "oversold": max(0, consumed - DEMAND),
    }


def main() -> int:
    print(f"bench: {common.WORKERS} workers x {BENCH_ROUNDS} rounds, "
          f"capacity == demand == {DEMAND}\n")
    rows = [run(m) for m in ("a", "b", "c")]
    labels = {"a": "a FOR UPDATE row", "b": "b atomic UPDATE", "c": "c SKIP LOCKED pool"}
    print(f"{'strategy':<20} {'ops/s':>8} {'p50 ms':>8} {'p95 ms':>8} "
          f"{'retries':>8} {'rejects':>8} {'oversold':>9}")
    bad = False
    for r in rows:
        if r["failed"]:
            print(f"{labels[r['mode']]:<20} {'FAILED':>8}")
            bad = True
            continue
        print(f"{labels[r['mode']]:<20} {r['ops_s']:>8.0f} {r['p50']:>8.2f} "
              f"{r['p95']:>8.2f} {r['retries']:>8} {r['rejects']:>8} {r['oversold']:>9}")
        bad = bad or r["oversold"] > 0
    return 1 if bad else 0


if __name__ == "__main__":
    sys.exit(main())
