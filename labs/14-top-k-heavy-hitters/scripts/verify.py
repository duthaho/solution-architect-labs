"""The invariant gate. Recomputes, never trusts a drill's own "ok" flag.

Normal mode, from the journals both drills leave behind:
  - the stream replays from its journaled seed; journaled evidence "true"
    counts and totals must match the replay exactly
  - CMS one-sided error: no journaled estimate below its true count
  - Space-Saving: zero guaranteed keys missing, no tracked underestimate
  - recall floor (0.95) on every properly-sized run; redis floor 0.80
  - mysql_rollup matched the oracle exactly
  - merge linearity RECOMPUTED: buckets + full sketch rebuilt from the
    journaled params, checksums must equal the journaled ones
  - CU divergence and the trap (missed naively, recovered widened) held

VERIFY_INVERT=1 inspects ONLY records flagged naive=true (the starved
sketch, the naive candidate policy) and exits 0 iff they VIOLATE the
production invariants — proof the gate catches the failure, not proof it
vacuously passes.
"""

import os
import sys
from collections import Counter

import common
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
            if r["missing"]:
                failures.append(f"space-saving: {r['missing']} guaranteed keys missing")
            if any(e["est"] < e["true"] for e in r["evidence"]):
                failures.append("space-saving underestimates a tracked key")
            if r["recall"] < RECALL_FLOOR:
                failures.append(f"space-saving recall {r['recall']:.3f} < {RECALL_FLOOR}")
        elif r["run"] == "redis_topk":
            if r["recall"] < REDIS_FLOOR:
                failures.append(f"redis topk recall {r['recall']:.3f} < {REDIS_FLOOR}")
        elif r["run"] == "mysql_rollup":
            if not r["exact_match"]:
                failures.append("mysql_rollup did not match the oracle exactly")
    return failures


def check_merge() -> list[str]:
    failures: list[str] = []
    lin = next((r for r in MERGE if r["run"] == "linearity"), None)
    trap = next((r for r in MERGE if r["run"] == "trap"), None)
    if lin is None or trap is None:
        return ["merge journal incomplete — run drill-merge"]

    buckets = [CountMinSketch(lin["width"], lin["depth"]) for _ in range(lin["minutes"])]
    full = CountMinSketch(lin["width"], lin["depth"])
    ref = buckets[0]
    n = lin["n_events"]
    for i, key in enumerate(common.zipf_stream(n_events=n, n_keys=lin["n_keys"],
                                               seed=lin["seed"])):
        h = ref.raw_hashes(key)
        buckets[common.minute_of(i, n, lin["minutes"])].update_hashed(h)
        full.update_hashed(h)
    merged = buckets[0]
    for b in buckets[1:]:
        merged = merged.merge(b)
    if merged.checksum() != lin["vanilla_merged"]:
        failures.append("recomputed merged checksum differs from journal")
    if full.checksum() != lin["vanilla_full"]:
        failures.append("recomputed full-stream checksum differs from journal")
    if merged.checksum() != full.checksum():
        failures.append("merge linearity broken on recomputation")
    if lin["cu_merged"] == lin["cu_full"]:
        failures.append("CU checksums identical — non-linearity not demonstrated")
    if not trap["recovered"]:
        failures.append("trap: widened candidates did not recover the day top-K")
    if not trap["naive_misses"]:
        failures.append("trap: naive policy did not miss (drill lost its point)")
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
