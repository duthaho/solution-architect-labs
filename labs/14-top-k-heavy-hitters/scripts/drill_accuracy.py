"""The accuracy drill: every contender vs the exact oracle, plus the
CMS width sweep from generous to starved — the precision cliff, measured.

One seeded Zipf stream feeds all contenders in a single pass. Sketch top-K
is scored offline against the full set of seen keys, which isolates sketch
error from candidate-tracking error (the heap construction is proven
equivalent in topk_stores' selftest and exercised in bench.py).

Journals evidence for verify.py: run params, per-key (true, estimate)
samples, the Space-Saving guarantee set, and per-run metrics. Starved-width
runs are flagged naive=true — VERIFY_INVERT inspects only those.

Exit 0 iff (all deterministic under SEED except redis, which gets a floor):
  - mysql_rollup top-K tie-aware-equals the oracle top-K
  - Space-Saving holds its guarantee (every key with f > N/m tracked)
  - CMS estimates never undercount on the evidence sample
  - generous CMS (width 2^17) recall@K >= 0.95; starved (2^9) < 0.75
  - conservative update's recall >= vanilla's at every sweep width
  - redis TOPK (HeavyKeeper) recall@K >= 0.80
"""

import sys
import time
from collections import Counter

import common
import contenders as cont
from sketches import CountMinSketch

GENEROUS_WIDTH = 1 << 17
SWEEP_WIDTHS = [1 << 15, 1 << 13, 1 << 11, 1 << 9]
STARVED_WIDTH = 1 << 9
SS_M = 4096
COLD_RANK = 5000

JOURNAL = common.journal_path("accuracy_runs")


def rank_overlap(approx: list[int], exact: list[int]) -> float:
    """Mean prefix agreement: avg_i |approx[:i] ∩ exact[:i]| / i."""
    total = 0.0
    a_seen: set[int] = set()
    e_seen: set[int] = set()
    for i, (a, e) in enumerate(zip(approx, exact), 1):
        a_seen.add(a)
        e_seen.add(e)
        total += len(a_seen & e_seen) / i
    return total / max(len(exact), 1)


def tie_aware_equal(
    got: list[tuple[int, int]], want: list[tuple[int, int]], truth: Counter
) -> bool:
    """Equal up to permutation within tied counts at the K-th boundary."""
    if [c for _, c in got] != [c for _, c in want]:
        return False
    boundary = want[-1][1]
    if {k for k, c in got if c > boundary} != {k for k, c in want if c > boundary}:
        return False
    return all(truth[k] == c for k, c in got)


def sketch_topk(sketch: CountMinSketch, keys: list[int], k: int) -> list[tuple[int, int]]:
    scored = [(key, sketch.estimate(key)) for key in keys]
    scored.sort(key=lambda kv: (-kv[1], kv[0]))
    return scored[:k]


def metrics(
    top: list[tuple[int, int]], exact_top: list[tuple[int, int]], truth: Counter, k: int
) -> dict:
    exact_keys = [key for key, _ in exact_top]
    top_keys = [key for key, _ in top]
    err = [abs(cnt - truth[key]) for key, cnt in top]
    return {
        "recall": len(set(top_keys) & set(exact_keys)) / k,
        "rank_overlap": round(rank_overlap(top_keys, exact_keys), 4),
        "mean_err": round(sum(err) / len(err), 1) if err else 0.0,
        "max_err": max(err) if err else 0,
    }


def evidence_sample(truth: Counter, sketch: CountMinSketch, keys: list[int]) -> list[dict]:
    return [{"key": key, "true": truth[key], "est": sketch.estimate(key)} for key in keys]


def main() -> int:
    n, k = common.N_EVENTS, common.TOP_K
    JOURNAL.unlink(missing_ok=True)

    sketches: dict[tuple[int, bool], CountMinSketch] = {}
    for width in [GENEROUS_WIDTH] + SWEEP_WIDTHS:
        for cu in (False, True):
            sketches[(width, cu)] = CountMinSketch(width, conservative=cu)
    ss = cont.SpaceSavingAdapter(k, m=SS_M)
    redis_c = cont.RedisTopK(k)
    mysql_c = cont.MysqlRollup(k)

    truth: Counter = Counter()
    ref = sketches[(GENEROUS_WIDTH, False)]
    t0 = time.monotonic()
    for i, key in enumerate(common.zipf_stream(n_events=n)):
        minute = common.minute_of(i, n)
        truth[key] += 1
        hashes = ref.raw_hashes(key)
        for sk in sketches.values():
            sk.update_hashed(hashes)
        ss.update(key, minute)
        redis_c.update(key, minute)
        mysql_c.update(key, minute)
    common.log.info(
        "stream done: %d events, %d distinct keys, %.1fs",
        n, len(truth), time.monotonic() - t0,
    )

    exact_sorted = sorted(truth.items(), key=lambda kv: (-kv[1], kv[0]))
    exact_top = exact_sorted[:k]
    seen_keys = list(truth.keys())
    ev_keys = [key for key, _ in exact_sorted[:200]] + [
        key for key, _ in exact_sorted[COLD_RANK : COLD_RANK + 100]
    ]

    common.append_jsonl(JOURNAL, {
        "run": "stream", "seed": common.SEED, "n_events": n,
        "n_keys": common.N_KEYS, "zipf_s": common.ZIPF_S, "k": k,
        "truth_total": sum(truth.values()), "distinct": len(truth),
    })

    failures: list[str] = []
    print(f"{'contender':<14} {'width':>7} {'recall@'+str(k):>10} {'rank_ovl':>9} "
          f"{'mean_err':>9} {'max_err':>8}")

    for (width, cu), sk in sorted(sketches.items(), key=lambda x: (-x[0][0], x[0][1])):
        name = "cms_cu" if cu else "cms"
        top = sketch_topk(sk, seen_keys, k)
        m = metrics(top, exact_top, truth, k)
        naive = width == STARVED_WIDTH
        common.append_jsonl(JOURNAL, {
            "run": "sketch", "contender": name, "width": width, "depth": sk.depth,
            "conservative": cu, "sketch_seed": sk.sketch_seed, "naive": naive,
            "seed": common.SEED, "n_events": n, "k": k, **m,
            "evidence": evidence_sample(truth, sk, ev_keys),
        })
        print(f"{name:<14} {width:>7} {m['recall']:>10.3f} {m['rank_overlap']:>9.3f} "
              f"{m['mean_err']:>9.1f} {m['max_err']:>8}")
        under = sum(1 for e in evidence_sample(truth, sk, ev_keys) if e["est"] < e["true"])
        if under:
            failures.append(f"{name} w={width}: {under} undercounts (one-sided error broken)")

    gen_recall = metrics(sketch_topk(sketches[(GENEROUS_WIDTH, False)], seen_keys, k),
                         exact_top, truth, k)["recall"]
    starved_recall = metrics(sketch_topk(sketches[(STARVED_WIDTH, False)], seen_keys, k),
                             exact_top, truth, k)["recall"]
    if gen_recall < 0.95:
        failures.append(f"generous cms recall {gen_recall:.3f} < 0.95")
    if starved_recall >= 0.75:
        failures.append(f"starved cms recall {starved_recall:.3f} >= 0.75 — no cliff")
    for width in SWEEP_WIDTHS:
        r_v = metrics(sketch_topk(sketches[(width, False)], seen_keys, k), exact_top, truth, k)["recall"]
        r_c = metrics(sketch_topk(sketches[(width, True)], seen_keys, k), exact_top, truth, k)["recall"]
        if r_c < r_v:
            failures.append(f"CU recall {r_c:.3f} < vanilla {r_v:.3f} at width {width}")

    ss_top = ss.topk(k)
    m = metrics(ss_top, exact_top, truth, k)
    threshold = n / SS_M
    guaranteed = [key for key, f in exact_sorted if f > threshold]
    missing = [key for key in guaranteed if key not in ss.store.counts]
    tracked_ev = [
        {"key": key, "true": truth[key], "est": ss.store.counts[key]}
        for key in ev_keys if key in ss.store.counts
    ]
    common.append_jsonl(JOURNAL, {
        "run": "spacesaving", "contender": "spacesaving", "m": SS_M, "naive": False,
        "seed": common.SEED, "n_events": n, "k": k, **m,
        "guaranteed": len(guaranteed), "missing": len(missing),
        "evidence": tracked_ev,
    })
    print(f"{'spacesaving':<14} {'m=' + str(SS_M):>7} {m['recall']:>10.3f} "
          f"{m['rank_overlap']:>9.3f} {m['mean_err']:>9.1f} {m['max_err']:>8}")
    if missing:
        failures.append(f"space-saving guarantee broken: {len(missing)}/{len(guaranteed)} absent")
    if any(e["est"] < e["true"] for e in tracked_ev):
        failures.append("space-saving underestimates a tracked key")

    redis_top = redis_c.topk(k)
    m = metrics(redis_top, exact_top, truth, k)
    common.append_jsonl(JOURNAL, {
        "run": "redis_topk", "contender": "redis_topk", "naive": False,
        "stochastic": True, "seed": common.SEED, "n_events": n, "k": k, **m,
    })
    print(f"{'redis_topk':<14} {'hk':>7} {m['recall']:>10.3f} {m['rank_overlap']:>9.3f} "
          f"{m['mean_err']:>9.1f} {m['max_err']:>8}  (stochastic decay)")
    if m["recall"] < 0.80:
        failures.append(f"redis TOPK recall {m['recall']:.3f} < 0.80")

    mysql_top = mysql_c.topk(k)
    m = metrics(mysql_top, exact_top, truth, k)
    exact_match = tie_aware_equal(mysql_top, exact_top, truth)
    common.append_jsonl(JOURNAL, {
        "run": "mysql_rollup", "contender": "mysql_rollup", "naive": False,
        "seed": common.SEED, "n_events": n, "k": k, **m, "exact_match": exact_match,
    })
    print(f"{'mysql_rollup':<14} {'sql':>7} {m['recall']:>10.3f} {m['rank_overlap']:>9.3f} "
          f"{m['mean_err']:>9.1f} {m['max_err']:>8}")
    if not exact_match:
        failures.append("mysql_rollup top-K != oracle top-K (should be exact)")

    for c in (ss, redis_c, mysql_c):
        c.close()

    for f in failures:
        common.log.error("drill-accuracy: %s", f)
    if failures:
        common.log.error("drill-accuracy FAILED (%d checks)", len(failures))
        return 1
    common.log.info(
        "drill-accuracy OK: generous recall %.3f, starved recall %.3f — "
        "the cliff, measured; mysql exact; space-saving guarantee held",
        gen_recall, starved_recall,
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
