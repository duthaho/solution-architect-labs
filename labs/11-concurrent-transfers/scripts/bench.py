"""Benchmark the four correct strategies under identical hot-account
contention and print the comparison table.

Each strategy gets a fresh reseed and the exact same workload shape as the
race drill (all workers debit the same hot row per round, distinct amounts
and destinations), sized up via BENCH_ROUNDS. Reported per strategy:

    ops/s      acked transfers per wall second
    p50/p95    per-transfer latency (ms), acked ops only
    retries    all re-attempts (version conflicts + lock victims)
    deadlocks  the 1213/1205 subset of those retries
    rejects    clean business rejections (insufficient funds)
    conserved  drill-level invariant verdict

The naive handler is deliberately absent: a benchmark column for a handler
that corrupts balances would invite exactly the wrong comparison.
"""
import os
import subprocess
import sys
import time
from decimal import Decimal

import common

BENCH_ROUNDS = int(os.environ.get("BENCH_ROUNDS", "40"))
WORKERS = int(os.environ.get("WORKERS", "8"))

STRATEGY_NAMES = {
    "a": "a pessimistic (FOR UPDATE)",
    "b": "b optimistic (version)",
    "c": "c atomic conditional",
    "d": "d append-only ledger",
}


def run_mode(mode: str) -> dict:
    subprocess.run(
        [sys.executable, common.LAB_DIR / "scripts" / "seed.py"],
        check=True, capture_output=True,
    )
    env = dict(os.environ, MODE=mode, ROUNDS=str(BENCH_ROUNDS),
               WORKERS=str(WORKERS))
    t0 = time.monotonic()
    proc = subprocess.run(
        [sys.executable, common.LAB_DIR / "scripts" / "drill_race.py"],
        env=env, capture_output=True, text=True,
    )
    wall = time.monotonic() - t0
    conserved = proc.returncode == 0

    recs = common.read_jsonl(common.journal_path(mode))
    acked = [r for r in recs if r["ok"]]
    rejects = len(recs) - len(acked)
    retries = sum(r["retries"] for r in recs)
    deadlocks = sum(r.get("deadlocks", 0) for r in recs)
    p50, p95 = common.percentiles([r["ms"] for r in acked])
    return {
        "mode": mode, "acked": len(acked), "rejects": rejects,
        "retries": retries, "deadlocks": deadlocks,
        "ops_s": len(acked) / wall if wall else 0.0,
        "p50": p50, "p95": p95, "conserved": conserved,
    }


def main() -> None:
    print(f"--- bench: {WORKERS} workers x {BENCH_ROUNDS} rounds per strategy, "
          f"fresh reseed each ---")
    rows = [run_mode(m) for m in ("a", "b", "c", "d")]
    hdr = (f"{'strategy':<28} {'acked':>6} {'rejects':>8} {'retries':>8} "
           f"{'deadlocks':>10} {'ops/s':>8} {'p50 ms':>8} {'p95 ms':>8} "
           f"{'conserved':>10}")
    print("\n" + hdr)
    print("-" * len(hdr))
    for r in rows:
        print(f"{STRATEGY_NAMES[r['mode']]:<28} {r['acked']:>6} "
              f"{r['rejects']:>8} {r['retries']:>8} {r['deadlocks']:>10} "
              f"{r['ops_s']:>8.1f} {r['p50']:>8.1f} {r['p95']:>8.1f} "
              f"{'yes' if r['conserved'] else 'NO':>10}")
    if not all(r["conserved"] for r in rows):
        sys.exit(1)


if __name__ == "__main__":
    main()
