"""Staleness auditor: joins the write journal (committed truth) against the
read journal (served values) and reports, per strategy:

    stale reads     served price != latest price committed before the read
    staleness       for each stale read, how long the truth had already been
                    different: read_ts - ts of the write that superseded the
                    served value. Max + percentiles. Ground truth, not vibes.

SLOP_MS absorbs journaling order noise: a read racing a concurrent commit can
legitimately serve the price of a write journaled a few ms after the read's
own stamp — that is concurrency, not staleness, and it is not counted.

--timeline N adds an N-second bucketed view (drill-cdc's outage bump).
--json merges this run's per-strategy metrics into audit_summary.json, which
report.py renders into the final money table.
"""
import argparse
import json
from bisect import bisect_right
from collections import defaultdict

from common import (LAB_DIR, LAG_JOURNAL, READ_JOURNAL, WRITE_JOURNAL, log,
                    read_journal)

SLOP_MS = 100
SUMMARY = LAB_DIR / "audit_summary.json"


def pct(sorted_vals: list, p: float) -> float:
    if not sorted_vals:
        return 0.0
    return sorted_vals[min(len(sorted_vals) - 1, int(p / 100 * len(sorted_vals)))]


def audit(strategy: str, writes: list[dict], reads: list[dict],
          timeline_s: int | None) -> dict | None:
    by_id: dict[int, list[tuple[int, float]]] = defaultdict(list)
    for w in writes:
        if w["strategy"] == strategy:
            by_id[w["id"]].append((w["ts"], w["price"]))
    for lst in by_id.values():
        lst.sort()
    my_reads = [r for r in reads if r["strategy"] == strategy]
    if not my_reads:
        return None

    stale: list[tuple[int, int]] = []   # (read_ts, staleness_ms)
    judged = hits = 0
    for r in my_reads:
        hits += r["source"].startswith("cache")
        hist = by_id.get(r["id"], [])
        i = bisect_right(hist, (r["ts"] + SLOP_MS, float("inf")))
        if i == 0:
            continue                     # only the seed value existed: unjudgeable
        judged += 1
        if r["price"] == hist[i - 1][1]:
            continue                     # fresh (modulo slop)
        # find the served value's origin write, then the write that superseded it
        origin = next((j for j in range(i - 1, -1, -1)
                       if hist[j][1] == r["price"] and hist[j][0] <= r["ts"]), None)
        superseder_ts = hist[origin + 1][0] if origin is not None else hist[0][0]
        stale.append((r["ts"], max(0, r["ts"] - superseder_ts)))

    lags = sorted(s for _, s in stale)
    result = {
        "reads": len(my_reads), "judged": judged,
        "hit_rate": round(100 * hits / len(my_reads), 1),
        "stale_count": len(stale),
        "stale_pct": round(100 * len(stale) / judged, 2) if judged else 0.0,
        "p50_ms": pct(lags, 50), "p99_ms": pct(lags, 99),
        "max_ms": lags[-1] if lags else 0,
    }
    print(f"\n=== strategy: {strategy} ===")
    print(f"  reads: {result['reads']}  (cache hit rate {result['hit_rate']}%, "
          f"{judged} judged against committed writes)")
    print(f"  stale reads: {result['stale_count']} ({result['stale_pct']}%)")
    print(f"  staleness   p50={result['p50_ms']}ms  p99={result['p99_ms']}ms  "
          f"MAX={result['max_ms']}ms")

    if timeline_s and my_reads:
        t0 = my_reads[0]["ts"]
        buckets: dict[int, list] = defaultdict(lambda: [0, 0, 0])  # reads, stale, max
        for r in my_reads:
            buckets[(r["ts"] - t0) // (timeline_s * 1000)][0] += 1
        for ts, lag in stale:
            b = buckets[(ts - t0) // (timeline_s * 1000)]
            b[1] += 1
            b[2] = max(b[2], lag)
        print(f"  timeline ({timeline_s}s buckets):")
        peak = max(b[2] for b in buckets.values()) or 1
        for k in sorted(buckets):
            reads_n, stale_n, mx = buckets[k]
            bar = "#" * round(40 * mx / peak)
            print(f"    t+{k * timeline_s:>3}s  reads={reads_n:<5} stale={stale_n:<5} "
                  f"max_staleness={mx:>6}ms {bar}")
    return result


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--strategy", choices=["ttl", "delete", "versioned", "cdc"],
                    help="default: every strategy present in the read journal")
    ap.add_argument("--timeline", type=int, metavar="SECONDS")
    ap.add_argument("--json", action="store_true",
                    help="merge metrics into audit_summary.json for report.py")
    ap.add_argument("--expect-max-at-least", type=int, metavar="MS",
                    help="exit 1 unless MAX staleness >= MS (drill gates)")
    args = ap.parse_args()

    writes, reads = read_journal(WRITE_JOURNAL), read_journal(READ_JOURNAL)
    if not reads:
        raise SystemExit("read journal is empty — run traffic.py (or make soak) first")
    strategies = [args.strategy] if args.strategy \
        else sorted({r["strategy"] for r in reads})

    summary = json.loads(SUMMARY.read_text()) if SUMMARY.exists() else {}
    results = {}
    for s in strategies:
        result = audit(s, writes, reads, args.timeline)
        if result:
            summary[s] = results[s] = result

    if "cdc" in strategies:
        lags = sorted(e["lag_ms"] for e in read_journal(LAG_JOURNAL))
        if lags:
            print(f"\n  cdc pipeline lag (binlog commit -> key deleted), "
                  f"{len(lags)} invalidations:")
            print(f"    p50={pct(lags, 50)}ms  p99={pct(lags, 99)}ms  MAX={lags[-1]}ms"
                  "   <- this, plus one cache miss, is the staleness bound")

    if args.json:
        SUMMARY.write_text(json.dumps(summary, indent=2))
        log.info("metrics merged into %s", SUMMARY.name)

    if args.expect_max_at_least is not None:
        worst = max((r["max_ms"] for r in results.values()), default=0)
        if worst < args.expect_max_at_least:
            log.error("❌ expected MAX staleness >= %dms, measured %dms",
                      args.expect_max_at_least, worst)
            raise SystemExit(1)


if __name__ == "__main__":
    main()
