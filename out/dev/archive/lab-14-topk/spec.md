# Lab 14 — Top-K most-viewed products (heavy hitters under Zipfian traffic)

## What & why

Build `labs/14-top-k-heavy-hitters/`: the P05 backlog problem — "top-K
most-viewed products: 10M products, 10B views/day, windows per
minute/day/month". The lab reproduces, on a laptop, the core tension: exact
counting is simple but doesn't survive per-window multiplicity and
mergeability at scale, while sketches trade bounded memory for measurable,
bounded error. The student sees the error, measures it, and watches it
collapse when a sketch is undersized.

New flavor for the repo: first lab about **approximate data structures** and
accuracy-as-an-invariant rather than consistency-as-an-invariant.

## Decisions

- **D1 — Topic**: P05 Top-K (backlog's 🎯 Next up). Lab number 14, slug
  `14-top-k-heavy-hitters`.
- **D2 — Full contender cast**, all fed the identical seeded event stream:
  1. `exact` — Python dict ground truth (memory baseline + oracle)
  2. `cms` — hand-rolled Count-Min Sketch + size-K min-heap
  3. `cms_cu` — CMS with conservative update
  4. `spacesaving` — hand-rolled Space-Saving (Misra-Gries family)
  5. `redis_topk` — Redis `TOPK.*` (HeavyKeeper, built into Redis 8 core)
  6. `mysql_rollup` — per-minute exact rollup table,
     `INSERT .. ON DUPLICATE KEY UPDATE cnt = cnt + 1` (batched), the
     "classic interview answer" contender
- **D3 — Stack**: MySQL 8.0.43 (port **3320**) + Redis 8 pinned (port
  **6392**), adminer on **8094** behind the `ui` profile. House compose
  conventions (healthchecks, named volumes, `lab14-*` container names).
- **D4 — Drills** (each deterministic, seeded RNG, dual naive/hardened
  structure with exact expected numbers):
  - `drill-accuracy` — seeded Zipf stream over a fixed key universe; exact
    oracle vs every sketch contender; reports recall@K, rank agreement, and
    max/mean count error; includes a CMS width sweep (generous → starved)
    showing the precision cliff, and shows conservative update moving the
    cliff.
  - `drill-merge` — 60 per-minute CMS buckets merged vs one full-stream
    sketch: **bit-identical** for vanilla CMS (strong assertion), **not**
    for conservative update (linearity caveat); plus the adversarial
    cross-window trap: a key that is top-3 for the "day" but never top-K in
    any single "minute", missed by naive union-of-per-minute-top-Ks,
    recovered by widened candidate sets re-scored against the merged sketch.
- **D5 — Bench**: `bench.py` runs every contender over the same stream;
  reports updates/s, p50/p95 update latency, memory footprint, and top-K
  accuracy vs oracle. Exit code asserts structural correctness only (never
  absolute throughput).
- **D6 — Verify gate**: `verify.py` with the house `VERIFY_INVERT=1`
  contract. Normal mode checks: oracle totals conserved (sum of all counts ==
  events emitted), CMS estimates never below true counts (one-sided error),
  Space-Saving guarantee (any key with f > N/m present), merged CMS ==
  full-stream CMS, recall@K of properly-sized sketches ≥ agreed floor.
  INVERT mode inspects only the starved-sketch run and must find violations.

## Out of scope

- Skew sweep drill and Redis restart/replay drill (parked; noted in README's
  production checklist as extensions).
- Sliding windows, streaming infra (Kafka/streams), month-scale rollups
  beyond the minute→hour→day merge story.
- Count Sketch (median estimator) — mentioned in README deep dive, not
  implemented.

## Assumptions

- **A1 — Scale-down**: key universe ~1M product ids, stream ~1–2M seeded
  events per drill (pure-Python-feasible in seconds); 10B/day framed as
  arithmetic in the README, not simulated.
- **A2 — Determinism**: single fixed seed per drill; fixed hash seeds for
  sketches so CMS merge assertions are bit-identical across runs.
- **A3 — "Minutes" are logical buckets** (event index ranges), not
  wall-clock — keeps drills instant and deterministic.
- **A4 — requirements.txt**: `PyMySQL==1.1.1`, `redis==5.0.8` only. Sketches
  hand-rolled (pedagogy); no numpy, no datasketches.
- **A5 — Redis image**: `redis:8.0.x` (exact pin chosen at build time after
  confirming the tag pulls and `TOPK.*` responds).

## Acceptance criteria

1. `make demo` in a clean checkout runs end-to-end (up → install →
   bootstrap → both drills → bench → verify) and exits 0.
2. `VERIFY_INVERT=1 make verify-naive` exits 0 (gate proves it catches the
   starved sketch), plain `make verify` exits 0 after the drills.
3. Both drills are deterministic: two consecutive runs print identical
   accuracy numbers.
4. README follows the house skeleton (problem → architecture → deep dive →
   runbook → bench table with captured numbers → production checklist → 10
   interview questions → file map).
5. Root `README.md` gains the lab-14 row; `BACKLOG.md` flips P05 to
   ✅ Covered with the lab-13-style summary note.
6. `make clean` returns the machine to pristine (containers, volumes,
   artifacts gone).

## End-to-end check

`cd labs/14-top-k-heavy-hitters && make demo && make clean` — exit 0, output
shows every gate passing.
