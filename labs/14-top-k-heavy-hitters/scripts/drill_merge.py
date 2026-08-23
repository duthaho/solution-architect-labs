"""The windowing drill: minute buckets, merging, and the cross-window trap.

Three sub-checks, all deterministic:
  A. Linearity: 60 per-minute vanilla CMS merged == one full-stream CMS,
     bit-identical (sha256 over the raw counter arrays).
  B. Conservative update is NOT linear: same construction, checksums differ
     — merging CU buckets silently loses the CU advantage.
  C. The trap: a key that is top-3 for the day but never top-K in any
     single minute. Naive union-of-per-minute-top-K misses it; widening the
     per-minute candidate sets to 2K and re-scoring against the merged
     sketch recovers the exact day top-K.

Journals params + checksums + result sets to merge_runs.jsonl so verify.py
can recompute A/B independently from the seed.

Exit 0 iff A holds, B shows divergence, C misses naively and recovers widened.
"""

import sys
from collections import Counter

import common
from sketches import CountMinSketch

N_EVENTS = int(common.os.environ.get("MERGE_EVENTS", "600000"))
N_KEYS = 200_000
WIDTH = 1 << 13
MINUTES = 60
TRAP_K = 10

JOURNAL = common.journal_path("merge_runs")


def build_buckets(conservative: bool) -> tuple[list[CountMinSketch], CountMinSketch]:
    buckets = [CountMinSketch(WIDTH, conservative=conservative) for _ in range(MINUTES)]
    full = CountMinSketch(WIDTH, conservative=conservative)
    ref = buckets[0]
    for i, key in enumerate(common.zipf_stream(n_events=N_EVENTS, n_keys=N_KEYS)):
        hashes = ref.raw_hashes(key)
        buckets[common.minute_of(i, N_EVENTS, MINUTES)].update_hashed(hashes)
        full.update_hashed(hashes)
    return buckets, full


def merge_all(buckets: list[CountMinSketch]) -> CountMinSketch:
    merged = buckets[0]
    for b in buckets[1:]:
        merged = merged.merge(b)
    return merged


def trap_stream() -> list[tuple[int, int]]:
    """(minute, key) events. Giants top every minute, 15 minute-local keys
    out-rank the steady key S in every window, yet S is day #3."""
    giants = [1, 2]
    steady = 3
    events: list[tuple[int, int]] = []
    for m in range(MINUTES):
        locals_ = [1000 + m * 100 + j for j in range(15)]
        for _ in range(40):
            events.extend((m, g) for g in giants)
        for k in locals_:
            events.extend((m, k) for _ in range(30))
        events.extend((m, steady) for _ in range(20))
    return events


def main() -> int:
    JOURNAL.unlink(missing_ok=True)
    failures: list[str] = []

    buckets, full = build_buckets(conservative=False)
    merged = merge_all(buckets)
    linear_ok = merged.checksum() == full.checksum()
    common.log.info("A: merged 60 vanilla buckets %s full-stream sketch",
                    "==" if linear_ok else "!=")
    if not linear_ok:
        failures.append("vanilla CMS merge not bit-identical to full stream")

    cu_buckets, cu_full = build_buckets(conservative=True)
    cu_merged = merge_all(cu_buckets)
    cu_diverges = cu_merged.checksum() != cu_full.checksum()
    common.log.info("B: merged CU buckets %s full-stream CU sketch (divergence expected)",
                    "!=" if cu_diverges else "==")
    if not cu_diverges:
        failures.append("CU merge unexpectedly bit-identical — non-linearity not shown")

    common.append_jsonl(JOURNAL, {
        "run": "linearity", "seed": common.SEED, "n_events": N_EVENTS,
        "n_keys": N_KEYS, "width": WIDTH, "depth": 4, "minutes": MINUTES,
        "sketch_seed": common.SKETCH_SEED,
        "vanilla_merged": merged.checksum(), "vanilla_full": full.checksum(),
        "cu_merged": cu_merged.checksum(), "cu_full": cu_full.checksum(),
        "linear_ok": linear_ok, "cu_diverges": cu_diverges,
    })

    events = trap_stream()
    truth: Counter = Counter(key for _, key in events)
    day_top = sorted(truth.items(), key=lambda kv: (-kv[1], kv[0]))[:TRAP_K]
    steady = 3
    assert steady in {k for k, _ in day_top[:3]}, "trap misbuilt: steady not day top-3"

    tb = [CountMinSketch(WIDTH) for _ in range(MINUTES)]
    minute_counts: list[Counter] = [Counter() for _ in range(MINUTES)]
    for m, key in events:
        tb[m].update(key)
        minute_counts[m][key] += 1
    tmerged = merge_all(tb)

    def per_minute_top(n: int) -> set[int]:
        cand: set[int] = set()
        for mc in minute_counts:
            cand |= {k for k, _ in sorted(mc.items(), key=lambda kv: (-kv[1], kv[0]))[:n]}
        return cand

    naive_cand = per_minute_top(TRAP_K)
    naive_top = sorted(
        ((k, tmerged.estimate(k)) for k in naive_cand), key=lambda kv: (-kv[1], kv[0])
    )[:TRAP_K]
    naive_misses = steady not in {k for k, _ in naive_top}
    common.log.info("C: naive union of per-minute top-%d %s the day #3 key",
                    TRAP_K, "MISSES" if naive_misses else "keeps")
    if not naive_misses:
        failures.append("trap not reproduced: naive union kept the steady key")

    wide_cand = per_minute_top(2 * TRAP_K)
    wide_top = sorted(
        ((k, tmerged.estimate(k)) for k in wide_cand), key=lambda kv: (-kv[1], kv[0])
    )[:TRAP_K]
    recovered = [k for k, _ in wide_top] == [k for k, _ in day_top]
    common.log.info("C: widened candidates (top-%d) re-scored on merged sketch %s",
                    2 * TRAP_K, "recover the exact day top-K" if recovered else "STILL WRONG")
    if not recovered:
        failures.append("widened candidates failed to recover the day top-K")

    common.append_jsonl(JOURNAL, {
        "run": "trap", "k": TRAP_K, "minutes": MINUTES, "width": WIDTH,
        "day_top": day_top, "naive_top": naive_top, "wide_top": wide_top,
        "steady_key": steady, "naive_misses": naive_misses, "recovered": recovered,
        "naive": naive_misses,  # the naive candidate policy is the broken run
    })

    for f in failures:
        common.log.error("drill-merge: %s", f)
    if failures:
        common.log.error("drill-merge FAILED (%d checks)", len(failures))
        return 1
    common.log.info("drill-merge OK: linearity proven, CU non-linearity shown, "
                    "trap reproduced and recovered")
    return 0


if __name__ == "__main__":
    sys.exit(main())
