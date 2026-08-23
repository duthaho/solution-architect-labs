# Plan — lab 15 Postgres live reshard

Repo has no unit-test framework; labs prove behavior by running drills and an
independent verify gate. "Red" per task = reproduce the failure/absence first
(command shown failing), "green" = the script/target passing. Commit per task.

- [x] **T1 — Replication spike (riskiest first).** Throwaway compose with 2
  `postgres:17` containers (`wal_level=logical`); on a toy table prove:
  row-filtered publication (`WHERE workspace_id % 2 = 0`, needs PG15+),
  `copy_data` initial sync, streaming after sync, wait-for-LSN catch-up —
  settle empirically which signal is reliable for logical subs: publisher
  `pg_stat_replication.replay_lsn` vs subscriber
  `pg_replication_origin_status.remote_lsn` — and a reverse
  subscription in the other direction after dropping the forward one. Record
  findings in `log.md`; delete spike files. [D5]
  *Verify:* each mechanism shown working via psql output.
- [x] **T2 — Scaffold.** `labs/15-postgres-live-reshard/`: `docker-compose.yml`
  (lab15-mono/shard0/shard1, postgres:17 pinned, `wal_level=logical`,
  healthchecks, named volumes, `PG_PORT`-style env ports), `Makefile`
  (`help/up/down/clean/install`), `requirements.txt` (pinned psycopg),
  `.gitignore`. [D5, D6, A1]
  *Verify:* `make up` → all 3 healthy; `make clean` pristine.
- [x] **T3 — Schema + common.** `sql/schema.sql` (single `docs` table, serial
  PK, `workspace_id int NOT NULL`, payload, `updated_at`, **plus unique index
  `(workspace_id, id)` + `REPLICA IDENTITY USING INDEX` — required before any
  pub/sub exists, on all 3 nodes; see T1 findings**), `scripts/common.py`
  (psycopg connection factory w/ env overrides, seeded RNG, jsonl helpers,
  shard_for(workspace_id)), `scripts/bootstrap.py` applies schema to all 3.
  [D5, D6, A2]
  *Verify:* `make bootstrap` then `\d docs` on all 3 containers.
- [x] **T4 — Seed + traffic.** `scripts/seed.py` (~500k seeded rows on mono),
  `scripts/traffic.py` (continuous seeded inserts/updates through
  `scripts/router.py`; every acked write journaled to `journal.jsonl`;
  router reads atomic `router_state.json`, write-gate flag for quiesce).
  [D3, A2]
  *Verify:* `make seed` row count; `make traffic-start`/`traffic-stop`
  journals acked writes routed to mono.
- [x] **T5 — Replication setup.** `scripts/replicate.py`: row-filtered
  publications on mono (per shard), subscriptions on shard0/1 with
  `copy_data`, `status` subcommand printing per-shard lag +
  `wait-lsn` subcommand (per T1: publisher `pg_stat_replication.replay_lsn
  >= captured LSN`, the only signal that advances under quiesce). Make targets
  `replicate`, `replicate-status`. [D2.2, D5]
  *Verify:* under live traffic, shard counts converge; `wait-lsn` returns
  only when `replay_lsn` passes captured LSN.
- [x] **T6 — Naive cutover drill.** `scripts/drill_naive.py`: under traffic,
  flip routing to shards *without* quiesce/LSN gate (optionally with an
  injected replication stall for determinism — disable subscription
  momentarily), then count journaled-acked rows missing/stale on shards.
  Make target `drill-naive`. [D2.1]
  *Verify:* deterministic ≥1 lost/stale row reported, journaled to
  `naive.jsonl`. Include a `reset-shards` target (drop subs, truncate
  shards, re-replicate) so the gated path starts clean — plain `down`/`up`
  keeps volumes and does NOT reset.
- [x] **T7 — Gated cutover.** `scripts/cutover.py`: gate write traffic at
  router → capture mono LSN → wait replay on both shards → flip
  `router_state.json` atomically → resume; print pause duration. Make
  target `cutover`. [D2.2]
  *Verify:* under traffic, 0 journaled-acked rows missing after cutover;
  pause duration printed (expect low seconds).
- [x] **T8 — Sequence drill.** `scripts/drill_sequence.py`: post-cutover
  insert on shards using the serial PK → duplicate-key error (sequences
  don't replicate); fix = **interleaved sequences** (`ALTER SEQUENCE …
  INCREMENT 2`, shard0 odd / shard1 even, restarted above `max(id)`) so the
  two writable shards can never mint the same ID — a prerequisite for T9's
  conflict-free reverse replication (README notes `setval(max)` alone as
  the trap, lab-13-style IDs as the production answer). Make target
  `drill-sequence`. [D2.3]
  *Verify:* deterministic duplicate-key error pre-fix, clean inserts
  post-fix, and disjoint ID sets minted by shard0 vs shard1.
- [x] **T9 — Rollback drill.** `scripts/drill_rollback.py`: after cutover
  *and after T8's interleaved-sequence fix* (collision-free IDs are the
  precondition for conflict-free reverse apply), drop forward subs, create
  reverse pubs on shards + subs on mono; take N journaled writes on shards;
  roll back (gate → wait reverse LSN → flip routing back); prove all N
  exist on mono. Make target `drill-rollback`. [D2.4]
  *Verify:* all post-cutover acked writes present on mono after rollback.
- [x] **T10 — Verify gate.** `scripts/verify.py`: **state-aware** — reads
  `router_state.json` to know who's authoritative. Row counts + per-range
  ordered-row md5 checksums comparing the *union of shard partitions*
  against mono (never per-shard vs whole-mono), plus routing-boundary check
  (each shard holds only rows its filter owns); sampled dark reads with
  replication-wait; journal replay (never trusts stored ok flags);
  `VERIFY_INVERT=1` passes only against naive-drill damage. Make targets
  `verify`, `verify-naive`. [D2.5]
  *Verify:* exit 0 happy path; `verify-naive` exit 0 against `naive.jsonl`,
  exit 1 otherwise.
- [x] **T11 — Index bench.** `scripts/bench_index.py`: time initial sync with
  secondary indexes kept vs dropped+rebuilt on the seeded table. Make
  target `bench-index`. [D4]
  *Verify:* bench completes and prints both timings (README cites the
  Notion/Figma expectation; no hard assertion on which wins — hardware
  varies).
- [x] **T12 — Demo target.** Makefile `demo`: clean-slate orchestration of
  the full story (up → bootstrap → seed → traffic → replicate → drill-naive
  → reset-shards → gated cutover → **verify** (post-cutover, shards
  authoritative) → drill-sequence → drill-rollback (does its own proof;
  verify's state-awareness [T10] handles mono-authoritative-again)),
  echo-delimited stages. [D6, AC1]
  *Verify:* `make demo` exit 0 from pristine state.
- [x] **T13 — Lab README.** `README.md` per repo outline (Problem →
  Architecture → Deep dives: why-not-double-writes, LSN gate, what logical
  replication doesn't carry, reverse-replication rollback → Runbook →
  Production checklist → Interview questions → File map), citing
  Notion/Figma/Slack posts. [D6]
  *Verify:* section outline matches lab 14's; runbook commands match
  Makefile targets exactly.
- [x] **T14 — Repo bookkeeping.** Root `README.md` lab-table row for 15;
  `BACKLOG.md` P14 → ✅ Covered with lab-15 note + parked drills (A4);
  recommendation section updated. [AC7]
  *Verify:* both files render consistently (P14 mapping mentions lab 15).
- [x] **T15 — End-to-end check (spec's final gate).**
  `make -C labs/15-postgres-live-reshard demo && make -C labs/15-postgres-live-reshard verify`
  from a clean checkout state, then `make clean` leaves machine pristine.
  [AC1–AC7]
