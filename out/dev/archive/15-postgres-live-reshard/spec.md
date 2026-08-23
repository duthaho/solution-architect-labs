# Lab 15 — Reshard a live Postgres with logical replication (P14)

## What & why

Split one overloaded Postgres into 2 shards under continuous traffic, with no
downtime, and keep a working rollback path *after* the cutover — the
Figma/Notion approach: **no double-writes; logical replication moves the data**,
an LSN catch-up gate + dark reads prove the copy, and a reverse replication
stream from new→old keeps rollback alive post-cutover.

Differentiates from lab 06 (MySQL, hand-rolled double-write + backfill +
shadow-read ladder): lab 15 is the approach Figma explicitly chose *because*
they rejected double-writes. First Postgres lab in the repo.

Sources: Notion "Sharding Postgres" + "The Great Re-shard", Figma "Growing
pains"/"Lived to tell the scale", Postgres logical-replication docs.

## Decisions

- **D1** Problem P14 from BACKLOG.md; ships as `labs/15-postgres-live-reshard/`.
- **D2** Drill set = Core 5:
  1. `drill-naive` — cut over while `replay_lsn < pg_current_wal_lsn()` under
     live writes → count acked-but-lost/stale rows on the shards.
  2. Gated cutover — quiesce writes → wait for LSN catch-up → flip routing →
     resume; prove zero acked-write loss, measure pause duration.
  3. `drill-sequence` — serial PK collides on the new shard after split
     (sequences don't replicate); fix with `setval()` re-seeding.
     *(Amended during T8/T9, recorded in log.md: `setval(max)` alone still
     lets two writable shards mint the same ids, which breaks the reverse
     replication D2.4 depends on — the shipped drill demonstrates `setval`
     as the trap and fixes with interleaved sequences, disjoint parity.)*
  4. `drill-rollback` — start reverse publication new→old right after cutover,
     take writes on new, roll back, prove post-cutover writes survived on old.
  5. `verify` — independent gate: row counts + per-range ordered-row checksums
     + sampled dark reads (with replication-wait), replayed from journals;
     `VERIFY_INVERT=1` proves the gate catches the naive drill's damage.
     *(Amended at the done gate: sampled dark reads are subsumed by a FULL
     journal replay — every acked write dark-read against the authoritative
     side — plus full-range checksums under a drained write gate; and
     `VERIFY_INVERT` recounts the damage against the shards themselves.)*
- **D3** Routing = app-side shard map (Notion-style): Python router, atomic
  rename on a routing-state JSON; pause = router-level write gate. No
  PgBouncer container.
- **D4** Include a small index-rebuild bench: initial logical-replication sync
  time with indexes kept vs dropped+rebuilt, on the seeded table.
- **D5** Topology: 3 Postgres containers (`lab15-mono`, `lab15-shard0`,
  `lab15-shard1`), shard key = `workspace_id` hashed % 2, publications on
  mono filtered per shard (row-filtered publications), subscriptions on each
  shard with `copy_data` initial sync.
- **D6** Repo conventions apply: self-contained `labs/15-*/` with
  docker-compose (healthchecks, `--wait`), Makefile (`help/up/down/clean/
  install/bootstrap/seed/traffic-*/.../verify/demo`), pinned requirements,
  seeded deterministic traffic, JSONL journals, `✅/❌` verify output, README
  with Problem → Architecture → Deep dives → Runbook → Production checklist →
  Interview questions → File map.

## Assumptions

- **A1** Postgres image: `postgres:17` pinned minor (latest stable at build
  time); driver `psycopg[binary]` pinned.
- **A2** Scale: ~500k seeded rows + continuous seeded write traffic — big
  enough for measurable sync/bench numbers, laptop-fast (matches lab 06).
- **A3** Cross-shard invariants out of scope: single table, every row owned by
  exactly one shard via `workspace_id` (Notion's model).
- **A4** Crash-mid-cutover, slot WAL-retention, long-txn stall, REPLICA
  IDENTITY drills: out of scope (parked in BACKLOG notes, per D2).

## Out of scope

- PgBouncer / proxy layer (D3), N>2 shards, schema changes during the split,
  cross-shard queries/transactions, the parked drills (A4).

## Acceptance criteria

1. `make demo` runs the full story end-to-end on a clean machine: up → seed →
   traffic → replicate → drills → gated cutover → rollback → verify, exit 0.
2. `drill-naive` deterministically shows ≥1 acked-but-missing row; the gated
   cutover shows 0 across repeated runs.
3. Sequence drill deterministically produces a duplicate-key error pre-fix,
   none post-fix.
4. Rollback drill proves every write acked after cutover exists on the old
   primary after rolling back.
5. `make verify` exits 0 on the happy path; `VERIFY_INVERT=1 make verify`
   exits 0 only against the naive journal.
6. Index bench prints kept-vs-dropped sync times.
7. README follows repo conventions; BACKLOG.md P14 flipped to ✅ Covered;
   README.md lab table row added.

## End-to-end check

`make -C labs/15-postgres-live-reshard demo && make -C labs/15-postgres-live-reshard verify`
