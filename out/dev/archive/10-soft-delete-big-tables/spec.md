# Spec — Lab 10: Soft delete for many big tables

## What & why

New lab `labs/10-soft-delete-big-tables/` in solution-architect-labs. Source problem
(case_study_01.txt #1): a microservice project decided that *every* table gets a
mirror "deleted" table in another schema. Is that sane at 100M+ rows under traffic?
The lab builds the three realistic strategies side by side on the same dataset,
makes each one's failure mode reproducible, and benchmarks them under live traffic.

The lab follows repo conventions exactly (template: lab 02): self-contained folder,
`docker-compose.yml`, `Makefile` with `make demo` / `make clean`, pinned
`requirements.txt`, individual Python scripts + `common.py`, README with the
7-section structure, failure drills in the runbook, interview questions.

## Decisions

- **D1** — MySQL 8.0.43 only (repo convention; container `lab10-mysql`).
  PostgreSQL partial indexes are discussed in the deep-dive as the road not taken,
  not run. *(user)*
- **D2** — Three strategies are runnable and benchmarked; partitioning
  (DROP PARTITION instead of DELETE) is deep-dive-only. *(user)*
  - **Strategy A — `deleted_at` column**: filter on every read. Demonstrates the
    real costs: broken unique constraints (deleted email blocks re-registration),
    the forgotten-WHERE bug, dead rows polluting indexes and scans.
  - **Strategy B — mirror deleted schema** (the case-study approach): schema
    `lab10_deleted` holds a structurally identical copy of every table; DELETE
    becomes move-row(s) in one transaction. Demonstrates multi-table cascade
    consistency, the restore (undelete) flow, and schema drift (ALTER the live
    table, forget the mirror → deletes start failing).
  - **Strategy C — soft delete + background archiver**: `deleted_at` for instant
    UX, then a background archiver moves flagged rows to an archive table in small
    batches (pt-archiver style: `LIMIT n`, pause, low lock footprint) plus a
    retention purge. Runs under live traffic; measures latency impact.
- **D3** — No ORM/Hibernate content anywhere in the lab. *(user)*
- **D4** — Root `README.md` lab table gets row 10 with status ✅.

## Assumptions

- **A1** — Schema family is 3 related tables (`users`, `orders`, `order_items`,
  FK-linked) so cross-table delete/restore pain is real, per "many big tables".
- **A2** — `SEED_ROWS ?= 500000` (orders; users/items scaled accordingly),
  overridable for "real pain" runs, per repo convention.
- **A3** — Strategy B's move is app-level in one transaction (what the Spring Boot
  service would do); DB-trigger variant is discussed in the deep-dive, not built.
- **A4** — Benchmark reports per strategy: delete p50/p95 latency, read p95
  (filtered vs unfiltered), live-table + index size after mass deletes, and
  restore correctness. Output is a printed comparison table at the end of
  `make demo`.
- **A5** — Verification invariant: every journaled row lives in exactly one place
  (live XOR deleted/archive), checked by `verify.py` after drills run under traffic.

## Out of scope

- ORM/JPA/Hibernate (D3).
- Runnable PostgreSQL or partitioning variants (D1, D2).
- GDPR/crypto-shredding, cross-service distributed deletes, backup/PITR interplay
  — production-checklist mentions only.
- Any change to existing labs 01–09.

## Acceptance criteria

1. `make demo` runs end-to-end on a clean machine (docker compose + venv): up →
   bootstrap → seed → traffic → all three strategies exercised → benchmark
   comparison printed → verify passes.
2. Each failure drill in the README reproduces its failure deterministically and
   each has a working recovery path: forgotten-WHERE bug (A), unique-key
   resurrection conflict (A), schema-drift move failure (B), archiver killed
   mid-batch resumes idempotently (C), one giant DELETE vs batched delete
   lock/latency comparison (C).
3. `verify.py` proves the A5 invariant against the traffic journal; non-zero exit
   on violation.
4. `make clean` returns the machine to pristine state.
5. README follows the repo's 7-section structure incl. production checklist
   ("what changes at 100M rows"), partitioning + Postgres partial-index deep-dive,
   8–10 interview questions.
6. Root README lab table lists lab 10 (D4).

## End-to-end check

`make -C labs/10-soft-delete-big-tables demo` completes with the comparison table
and `verify: OK`; then `make clean` leaves no containers/volumes/state files.
