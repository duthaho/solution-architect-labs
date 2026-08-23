# Plan — Lab 14 Top-K heavy hitters

Spec: `out/dev/lab-14-topk/spec.md`. Lab dir: `labs/14-top-k-heavy-hitters/`.

Testing note: this repo has no pytest suite — the house "tests" are
exit-code selftests, drills, and the verify gate. Red/green adapts to that:
write the assertion (selftest/drill exit contract) first, see it fail, then
implement until it exits 0.

- [x] **T1 — Scaffold + Redis 8 risk check (riskiest first)** [D3, A5]
      Files: `labs/14-top-k-heavy-hitters/{docker-compose.yml,.gitignore,requirements.txt}`, `sql/schema.sql`.
      Compose: mysql 8.0.43 :3320, redis 8 (pin exact tag) :6392, adminer :8094
      (`ui` profile), `lab14-*` container names, house healthchecks.
      Schema: `lab14` db + `views_minute (product_id, minute_no, cnt, PRIMARY KEY (minute_no, product_id))`.
      Verify: `docker compose up -d --wait mysql redis` then
      `docker exec lab14-redis redis-cli TOPK.RESERVE probe 10` returns OK.
      Research says TOPK is in core Redis 8 (8.0 release notes); codex
      review disputes this — the probe settles it empirically. Fallback if
      it fails: `redis/redis-stack-server:7.4.0-v3` (ships RedisBloom),
      same port; record whichever won in log.md. Must be settled before T6.

- [x] **T2 — common.py** [A1, A2, A3]
      Files: `scripts/common.py`.
      Ports 3320/6392, `DB="lab14"`, house helpers (connect with FOUND_ROWS,
      redis_client, wait_for_mysql, append/read_jsonl, percentiles, log) plus:
      seeded Zipf event generator (inverse-CDF over precomputed cumulative
      weights, fixed key universe, `SEED` env default 14), fixed sketch hash
      seeds, `minute_of(event_index)` logical bucketing.
      Verify: generating 100k events twice yields an identical checksum
      (one-liner via `python -c`).

- [x] **T3 — Makefile skeleton + bootstrap** [D3]
      Files: `Makefile`, `scripts/bootstrap.py`.
      Targets: help/up/down/clean/install/bootstrap in house style
      (`SHELL`, `VENV`, `PY`, exported ports). bootstrap.py reads
      `sql/schema.sql`, strips `--` comments, executes statement-by-statement
      autocommit (lab-13 pattern) — compose never loads SQL itself.
      Verify: `make up install bootstrap` exits 0; `views_minute` exists.

- [x] **T4 — sketches.py: CMS + conservative update** [D2, A2]
      Files: `scripts/sketches.py`.
      One class, `conservative=False|True`; update, point query (min),
      element-wise merge (rejects mismatched shape/seeds), `memory_bytes()`.
      Selftest mode (run as script): one-sided error on a known stream, merge
      of two halves == full-stream sketch bit-identically, CU strictly lower
      total error than vanilla on a colliding stream; `SELFTEST_BREAK=1`
      flips a hash seed and must FAIL (non-vacuous, lab-13 style).
      Verify: `SELFTEST_BREAK=1 python scripts/sketches.py` exits 1;
      plain run exits 0.

- [x] **T5 — topk_stores.py: Space-Saving + heap top-K** [D2]
      Files: `scripts/topk_stores.py`.
      Space-Saving with m counters (evict-min, inherit count, track error);
      `TopKHeap` maintaining size-K heap over CMS estimates.
      Selftest: Space-Saving guarantee — every key with f > N/m present —
      on a seeded stream; heap top-K == exact top-K when CMS is generous.
      Verify: `python scripts/topk_stores.py` exits 0.

- [x] **T6 — contenders.py: uniform adapter for all six** [D2, D3]
      Files: `scripts/contenders.py`.
      Interface: `name`, `update(key, minute_no)` (+ optional
      `update_batch(pairs)`), `topk(K)`, `memory_bytes()`, `close()` —
      minute_no is the logical bucket from `common.minute_of()`; in-memory
      contenders may ignore it, mysql_rollup keys on it. Adapters: exact dict, cms,
      cms_cu, spacesaving, redis_topk (`TOPK.RESERVE/ADD/LIST WITHCOUNT`),
      mysql_rollup (batched `INSERT .. ON DUPLICATE KEY UPDATE`, top-K via
      `GROUP BY product_id ORDER BY SUM(cnt) DESC LIMIT K`).
      Verify: smoke script mode — 10k events through every adapter, each
      returns a plausible top-10, exit 0.

- [x] **T7 — drill_accuracy.py + targets** [D4, D6 groundwork] *(Makefile target lands with T9's verify targets)*
      Files: `scripts/drill_accuracy.py`, `Makefile` (`drill-accuracy`).
      Seeded Zipf stream (~1M events, ~1M-key universe) → exact oracle +
      ALL contenders including redis_topk and mysql_rollup; report recall@K,
      rank agreement, mean/max count error; assert mysql_rollup top-K ==
      oracle top-K exactly (it's exact by construction). CMS width sweep
      generous→starved (halving steps): the precision cliff, and CU moving
      it. Journal per-run metrics PLUS verification evidence to
      `accuracy_*.jsonl`: run params (seed, w, d, hash seeds, N, m), a
      deterministic per-key evidence sample (true count + each contender's
      estimate for the oracle's top-200 and a fixed slice of cold keys), and
      the Space-Saving f > N/m key set; starved runs flagged `naive: true`.
      Exit 0 iff properly-sized recall@100 ≥ floor AND starved recall <
      floor — both deterministic under the fixed seed.
      Verify: two consecutive runs print identical numbers.

- [x] **T8 — drill_merge.py + target** [D4]
      Files: `scripts/drill_merge.py`, `Makefile` (`drill-merge`).
      60 logical minute-buckets: (a) vanilla CMS merged == full-stream CMS
      bit-identical (assert on raw counter arrays); (b) CU merged !=
      full-stream CU (linearity caveat caught); (c) adversarial stream with
      a key ranked ~K+5 every minute but top-3 for the day — naive union of
      per-minute top-Ks misses it, widened candidates (top-2K) re-scored
      against the merged sketch recover it. Journal to `merge_*.jsonl`:
      sub-check results plus run params (seed, w, d, hash seeds) and
      counter-array checksums so verify can recompute independently.
      Exit 0 iff all three sub-checks land exactly as scripted.
      Verify: deterministic across two runs.

- [x] **T9 — verify.py + verify/verify-naive targets** [D6]
      Files: `scripts/verify.py`, `Makefile`.
      Normal, from journaled evidence + recomputation (never a trusted
      "ok" flag): oracle count conservation (sum of evidence-sample true
      counts consistent with journaled N), CMS estimate ≥ true count for
      every journaled evidence pair, Space-Saving f > N/m key set fully
      present in its journaled top output, merge identity RECOMPUTED by
      re-deriving both sketches from the journaled seed/params and comparing
      counter checksums, recall floor on properly-sized runs; fails on
      empty/missing journals.
      `VERIFY_INVERT=1`: inspects ONLY `naive: true` (starved) records, exit
      0 iff violations found.
      Verify: after T7+T8, `make verify` → 0 and `make verify-naive` → 0.

- [x] **T10 — bench.py + target** [D5]
      Files: `scripts/bench.py`, `Makefile` (`bench`).
      All six contenders, identical stream (fresh MySQL truncate + fresh
      TOPK key per run): updates/s, p50/p95 update latency, memory,
      recall@100 vs oracle; fixed-width table. Exit asserts structure only
      (every contender completes, exact contender exact, no exceptions).
      Verify: `make bench` exits 0, table prints.

- [x] **T11 — demo target + determinism check** [acceptance 1, 3]
      Files: `Makefile` (`demo`).
      Chain in narrative order: up install bootstrap → drill-accuracy →
      verify-naive → drill-merge → verify → bench, numbered `===` echoes,
      lab-13 style. Verify: `make demo` twice from `make clean`; exit 0 both
      times, accuracy numbers identical.

- [x] **T12 — Lab README.md** [acceptance 4]
      Files: `labs/14-top-k-heavy-hitters/README.md`.
      House skeleton: problem → architecture (ASCII) → deep dive (CMS
      bounds & the εN intuition, conservative update, Space-Saving
      guarantee, HeavyKeeper decay, windowing/merge, the cross-window trap,
      the 10B/day arithmetic) → runbook → captured bench table + captured
      drill numbers → production checklist (incl. parked drills, sliding
      windows, Count Sketch mention) → 10 interview questions → file map.
      Cite Cormode&Muthukrishnan, Cormode&Hadjieleftheriou VLDB'08,
      Metwally'05, HeavyKeeper ATC'18, Redis TOPK docs.
      Verify: section order matches labs 12/13; numbers are pasted from real
      runs, not invented.

- [x] **T13 — Root README row + BACKLOG flip** [acceptance 5]
      Files: `README.md`, `BACKLOG.md`.
      Add lab 14 row (category Scale / Analytics); flip P05 to ✅ Covered
      with a lab-13-style summary note; update the "what to build next"
      recommendation section.
      Verify: both tables render; P05 note names Lab 14.

- [x] **T14 — End-to-end check (the spec's final gate)** [acceptance 1, 6]
      `cd labs/14-top-k-heavy-hitters && make clean && make demo && make clean`
      from a pristine state — exit 0, every gate green. Append final log.md
      entry; then run the done gate.
