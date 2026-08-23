"""The invariant gate. Recomputes what a drill could have journaled wrong.

Normal mode, from the journals both drills leave behind — RECOMPUTED from
the journaled seeds/params, never read back as a trusted "ok" flag:
  - the stream replays from its journaled seed; journaled evidence "true"
    counts and totals must match the replay exactly
  - CMS one-sided error: no journaled estimate below its replayed true count
  - Space-Saving guarantee re-derived: every replayed-truth key with
    f > N/m must be in the journaled tracked set
  - mysql_rollup's journaled top-K re-checked tie-aware against the replay
  - merge linearity: vanilla AND CU buckets + full sketches rebuilt from
    the journaled seed/zipf_s/sketch_seed; checksums must match the journal
    and vanilla must merge bit-identically while CU diverges
  - the trap rebuilt from scratch: naive union must miss, widened recover

The only journal-trusted numbers are the per-run recall/rank metrics,
which are held to floors (0.95; redis 0.80 — HeavyKeeper decay is
genuinely stochastic).

VERIFY_INVERT=1 inspects ONLY records flagged naive=true (the starved
sketch, the naive candidate policy) and exits 0 iff they VIOLATE the
production invariants — proof the gate catches the failure, not proof it
vacuously passes.
"""

import os
import sys
from collections import Counter

import common
from drill_accuracy import tie_aware_equal
from drill_merge import trap_stream
from sketches import CountMinSketch

INVERT = os.environ.get("VERIFY_INVERT") == "1"
RECALL_FLOOR = 0.95
REDIS_FLOOR = 0.80

ACC = common.read_jsonl(common.journal_path("accuracy_runs"))
MERGE = common.read_jsonl(common.journal_path("merge_runs"))


def replay_truth(stream_rec: dict) -> Counter:
    truth: Counter = Counter()
    for key in common.zipf_stream(
        n_events=stream_rec["n_events"],
        n_keys=stream_rec["n_keys"],
        s=stream_rec["zipf_s"],
        seed=stream_rec["seed"],
    ):
        truth[key] += 1
    return truth


def check_accuracy() -> list[str]:
    failures: list[str] = []
    stream = next((r for r in ACC if r["run"] == "stream"), None)
    if stream is None:
        return ["accuracy journal missing its stream record — run drill-accuracy"]
    if stream["truth_total"] != stream["n_events"]:
        failures.append("count conservation broken: truth_total != n_events")

    truth = replay_truth(stream)
    if len(truth) != stream["distinct"]:
        failures.append("replayed stream distinct-key count differs from journal")

    for r in ACC:
        if r["run"] == "stream" or r.get("naive"):
            continue
        for e in r.get("evidence", []):
            if truth[e["key"]] != e["true"]:
                failures.append(f"{r['contender']}: journaled true count is not the "
                                f"replayed truth for key {e['key']}")
                break
        if r["run"] == "sketch":
            under = sum(1 for e in r["evidence"] if e["est"] < e["true"])
            if under:
                failures.append(f"{r['contender']} w={r['width']}: {under} undercounts")
            if r["recall"] < RECALL_FLOOR:
                failures.append(f"{r['contender']} w={r['width']}: recall "
                                f"{r['recall']:.3f} < {RECALL_FLOOR}")
        elif r["run"] == "spacesaving":
            tracked = set(r["tracked"])
            threshold = r["n_events"] / r["m"]
            missing = [key for key, f in truth.items() if f > threshold and key not in tracked]
            if missing:
                failures.append(f"space-saving: {len(missing)} guaranteed keys "
                                "absent from the journaled tracked set")
            if any(e["est"] < e["true"] for e in r["evidence"]):
                failures.append("space-saving underestimates a tracked key")
            if r["recall"] < RECALL_FLOOR:
                failures.append(f"space-saving recall {r['recall']:.3f} < {RECALL_FLOOR}")
        elif r["run"] == "redis_topk":
            if r["recall"] < REDIS_FLOOR:
                failures.append(f"redis topk recall {r['recall']:.3f} < {REDIS_FLOOR}")
        elif r["run"] == "mysql_rollup":
            exact_top = sorted(truth.items(), key=lambda kv: (-kv[1], kv[0]))[: r["k"]]
            got = [(key, cnt) for key, cnt in r["top"]]
            if not tie_aware_equal(got, exact_top, truth):
                failures.append("mysql_rollup journaled top-K does not match the replay")
    return failures


def check_merge() -> list[str]:
    failures: list[str] = []
    lin = next((r for r in MERGE if r["run"] == "linearity"), None)
    trap = next((r for r in MERGE if r["run"] == "trap"), None)
    if lin is None or trap is None:
        return ["merge journal incomplete — run drill-merge"]

    def build(conservative: bool):
        buckets = [
            CountMinSketch(lin["width"], lin["depth"], conservative, lin["sketch_seed"])
            for _ in range(lin["minutes"])
        ]
        full = CountMinSketch(lin["width"], lin["depth"], conservative, lin["sketch_seed"])
        ref = buckets[0]
        n = lin["n_events"]
        for i, key in enumerate(common.zipf_stream(
            n_events=n, n_keys=lin["n_keys"], s=lin["zipf_s"], seed=lin["seed"]
        )):
            h = ref.raw_hashes(key)
            buckets[common.minute_of(i, n, lin["minutes"])].update_hashed(h)
            full.update_hashed(h)
        merged = buckets[0]
        for b in buckets[1:]:
            merged = merged.merge(b)
        return merged, full

    merged, full = build(conservative=False)
    if merged.checksum() != lin["vanilla_merged"]:
        failures.append("recomputed merged checksum differs from journal")
    if full.checksum() != lin["vanilla_full"]:
        failures.append("recomputed full-stream checksum differs from journal")
    if merged.checksum() != full.checksum():
        failures.append("merge linearity broken on recomputation")

    cu_merged, cu_full = build(conservative=True)
    if cu_merged.checksum() != lin["cu_merged"] or cu_full.checksum() != lin["cu_full"]:
        failures.append("recomputed CU checksums differ from journal")
    if cu_merged.checksum() == cu_full.checksum():
        failures.append("CU checksums identical — non-linearity not demonstrated")

    events = trap_stream()
    truth = Counter(key for _, key in events)
    k = trap["k"]
    day_top = sorted(truth.items(), key=lambda kv: (-kv[1], kv[0]))[:k]
    tb = [CountMinSketch(trap["width"], sketch_seed=trap["sketch_seed"])
          for _ in range(trap["minutes"])]
    minute_counts = [Counter() for _ in range(trap["minutes"])]
    for m, key in events:
        tb[m].update(key)
        minute_counts[m][key] += 1
    tmerged = tb[0]
    for b in tb[1:]:
        tmerged = tmerged.merge(b)

    def union_top(n: int) -> list[tuple[int, int]]:
        cand: set[int] = set()
        for mc in minute_counts:
            cand |= {kk for kk, _ in sorted(mc.items(), key=lambda kv: (-kv[1], kv[0]))[:n]}
        return sorted(((kk, tmerged.estimate(kk)) for kk in cand),
                      key=lambda kv: (-kv[1], kv[0]))[:k]

    steady = trap["steady_key"]
    if steady in {kk for kk, _ in union_top(k)}:
        failures.append("trap recomputation: naive policy did not miss the steady key")
    if [kk for kk, _ in union_top(2 * k)] != [kk for kk, _ in day_top]:
        failures.append("trap recomputation: widened candidates did not recover day top-K")
    return failures


def check_naive_violations() -> list[str]:
    """INVERT: production invariants that the naive records rightly break."""
    violations: list[str] = []
    for r in ACC:
        if r.get("naive") and r["run"] == "sketch":
            if r["recall"] < RECALL_FLOOR:
                violations.append(f"starved {r['contender']} w={r['width']}: recall "
                                  f"{r['recall']:.3f} below production floor")
    trap = next((r for r in MERGE if r["run"] == "trap"), None)
    if trap and trap.get("naive") and trap["naive_misses"]:
        violations.append("naive union-of-top-K dropped a day top-3 key")
    return violations


def main() -> int:
    if not ACC or not MERGE:
        common.log.error("verify: journals missing — run both drills first")
        return 1

    if INVERT:
        violations = check_naive_violations()
        for v in violations:
            common.log.info("caught (expected): %s", v)
        if violations:
            common.log.info("verify(INVERT): %d violations found — the gate catches "
                            "the starved sketch and the naive policy — exit 0",
                            len(violations))
            return 0
        common.log.error("verify(INVERT): no violations in naive records — gate is vacuous")
        return 1

    failures = check_accuracy() + check_merge()
    for f in failures:
        common.log.error("violation: %s", f)
    if not failures:
        common.log.info("verify: replayed truth matches, one-sided error holds, "
                        "guarantees hold, merge identity recomputed — exit 0")
        return 0
    common.log.error("verify: %d violations", len(failures))
    return 1


if __name__ == "__main__":
    sys.exit(main())
