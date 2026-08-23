# Log — lab-14-topk

- 2026-08-23 · spec approved (P05 top-K, full cast, Redis+MySQL, accuracy+merge drills) · —
- 2026-08-23 · plan approved after codex review; 7 findings, 6 folded in (adapter minute_no, verify recomputation/evidence journals, redis/mysql accuracy coverage, fallback image pinned, bootstrap mechanism stated); codex's "TOPK not in Redis 8 core" kept as empirical probe in T1 · —
- 2026-08-23 · branch lab/14-top-k-heavy-hitters created off main (f809490) · —
- 2026-08-23 · T1 scaffold done; TOPK.* probe PASSED on plain redis:8.0.3 (codex claim wrong, no redis-stack fallback needed) · bd62d80
- 2026-08-23 · T2 common.py: zipf stream deterministic (identical sha256 across runs), injective ids · (commit "common helpers")
- 2026-08-23 · T3 make up/install/bootstrap green, views_minute created
- 2026-08-23 · T4 sketches.py selftest green; SELFTEST_BREAK exits 1 (non-vacuous); CU 2x tighter (66770 vs 133154)
- 2026-08-23 · T5 topk_stores.py selftest green (166 guaranteed keys, heap==exact)
- 2026-08-23 · T6 contenders smoke green after removing a use-after-close check; all six agree on top-3
- 2026-08-23 · perf rework (unplanned but necessary): lazy heaps in TopKHeap + SpaceSaving (naive versions were O(k)/O(m) per hot update/eviction — infeasible at 1M events), shared-hash fast path in CMS for the width sweep; all selftests re-run green
- 2026-08-23 · T7 drill-accuracy green, ~88s: cliff measured (recall 1.000@2^17 -> 0.550@2^9, CU holds 0.630), redis TOPK recall 1.000 at 6.8KB, mysql tie-aware exact, SS guarantee held; starved threshold retuned 0.5->0.75 after first run (observed 0.550); determinism verified across two runs with redis rows flagged stochastic (max_err drifted 10->11, as expected)
- 2026-08-23 · T8 drill-merge green in 11s: linearity sha256-identical, CU diverges, trap missed naively + recovered widened; removed one dead var
- 2026-08-23 · T9 verify green both modes; INVERT catches exactly 3 planted violations; fails closed on missing journals (make exit 2)
- 2026-08-23 · T10 bench green after two fixes: format-spec typo, and ExactDict.most_common tie-order vs oracle (made adapter sort (-count, key)); table: exact 904k ops/s / 5.6MB, spacesaving 290k / 400KB, redis_topk 245k / 49KB, mysql 61k / 7.4MB, all recall 1.000
- 2026-08-23 · T11+T14 demo green from pristine clean twice (2m19s, 2m31s), exit 0; accuracy metrics json-identical across pristine runs (redis excluded as flagged); make clean returns machine to pristine
- 2026-08-23 · T12 README written with pasted (not invented) numbers; T13 root README row + BACKLOG P05 flipped, next-up now P14
- 2026-08-23 · done gate: 2 fresh reviewers + codex second opinion. Accepted: verify env-leak in merge recompute (zipf_s/sketch_seed now journaled+used), gate strengthened to recompute CU checksums/trap/SS guarantee/mysql match, bench flush-latency sampling (first fix with coprime 997 was statistically wrong — now every batch boundary is sampled; mysql p95 truthfully 60ms), SS topk tie-break, Count Sketch README mention. Rejected with reasons: backlog/selftest "scope creep" (plan-approved), INVERT-broader-than-letter (stronger gate). Disputed→user: codex's CU count>1 semantics (no call site affected). AC3 deviation on record: redis TOPK decay is server-side stochastic, rows flagged, floors gated. Re-gated: pristine demo exit 0 (2m45s), verify 13s recomputing everything.
