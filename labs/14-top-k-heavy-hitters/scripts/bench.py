"""Benchmark all six contenders under the identical seeded stream.

Reports updates/s (wall-clock over the whole stream, including each
contender's own batching), per-update p50/p95 sampled on every ~1000th
event PLUS every batch-flushing event — without the latter, the batched
contenders' tail latency would be pure list.append and structurally wrong —
memory footprint, and recall@K against the exact oracle.

Exit 0 iff structural assertions pass — never on absolute throughput:
  - every contender completes the stream and answers a full top-K
  - the exact contender's top-K is the oracle
  - mysql_rollup tie-aware-matches the oracle
  - every count in every top-K is positive

Absolute numbers vary by hardware; the comparison shape is stable.
"""

import sys
import time
from collections import Counter

import common
import contenders as cont
from drill_accuracy import metrics, tie_aware_equal

BENCH_EVENTS = int(common.os.environ.get("BENCH_EVENTS", "300000"))
SAMPLE_EVERY = 997


def bench_one(c, stream: list[int], k: int) -> dict:
    lats: list[float] = []
    t0 = time.monotonic()
    n = len(stream)
    for i, key in enumerate(stream):
        minute = common.minute_of(i, n)
        # sample the cadence AND every batch boundary: flushes are the tail
        if i % SAMPLE_EVERY == 0 or (i + 1) % cont.MYSQL_BATCH == 0:
            s = time.perf_counter_ns()
            c.update(key, minute)
            lats.append((time.perf_counter_ns() - s) / 1000)
        else:
            c.update(key, minute)
    if hasattr(c, "flush"):
        c.flush()
    wall = time.monotonic() - t0
    top = c.topk(k)
    p50, p95 = common.percentiles(lats)
    return {
        "name": c.name,
        "ops_s": len(stream) / wall,
        "p50_us": p50,
        "p95_us": p95,
        "mem": c.memory_bytes(),
        "top": top,
    }


def main() -> int:
    k = common.TOP_K
    stream = list(common.zipf_stream(n_events=BENCH_EVENTS))
    truth = Counter(stream)
    exact_top = sorted(truth.items(), key=lambda kv: (-kv[1], kv[0]))[:k]

    rows = []
    for c in cont.build_all(k):
        rows.append(bench_one(c, stream, k))
        c.close()

    failures: list[str] = []
    print(f"{'contender':<14} {'ops/s':>10} {'p50 µs':>8} {'p95 µs':>8} "
          f"{'memory':>10} {'recall@' + str(k):>10}")
    for r in rows:
        m = metrics(r["top"], exact_top, truth, k)
        print(f"{r['name']:<14} {r['ops_s']:>10.0f} {r['p50_us']:>8.1f} "
              f"{r['p95_us']:>8.1f} {r['mem']:>9d}B {m['recall']:>10.3f}")
        if len(r["top"]) != k or any(cnt <= 0 for _, cnt in r["top"]):
            failures.append(f"{r['name']}: incomplete or non-positive top-{k}")
        if r["name"] == "exact" and r["top"] != exact_top:
            failures.append("exact contender diverges from the oracle")
        if r["name"] == "mysql_rollup" and not tie_aware_equal(r["top"], exact_top, truth):
            failures.append("mysql_rollup top-K != oracle top-K")

    for f in failures:
        common.log.error("bench: %s", f)
    if failures:
        return 1
    common.log.info("bench OK: all six completed %d events; structure verified", BENCH_EVENTS)
    return 0


if __name__ == "__main__":
    sys.exit(main())
