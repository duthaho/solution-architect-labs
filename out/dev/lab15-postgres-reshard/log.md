# Log — lab 15 Postgres live reshard

- 2026-08-23 · spec approved (P14, core-5 drills, app-side routing, index bench)
- 2026-08-23 · plan approved; codex cross-model review: 7 findings, all folded into plan (LSN signal spike, reset-shards, interleaved sequences, union checksums, state-aware verify, bench no hard assert)
- 2026-08-23 · T1 spike done (postgres:17.11, throwaway containers, deleted). Findings:
  - Row-filtered pub `WHERE (workspace_id % 2 = 0)` + copy_data initial sync + streaming: all work; 0 filter leaks.
  - **LSN gate = publisher-side** `pg_stat_replication.replay_lsn >= captured pg_current_wal_lsn()`. Subscriber `pg_replication_origin_status.remote_lsn` STALLS below captured LSN when only non-logical WAL follows (checkpoint test: pub-side true, sub-side false) — opposite of codex's worry.
  - **Critical schema requirement:** a row-filtered pub that publishes UPDATE/DELETE requires filter columns ⊆ replica identity, else EVERY update on the table errors `cannot update table "t"` (would break traffic the moment pubs are created). Fix: unique index `(workspace_id, id)` + `REPLICA IDENTITY USING INDEX` — and it must be on ALL nodes (subscriber expects identity columns the publisher sends: `publisher did not send replica identity column` otherwise), and must be set BEFORE any pub/sub exists (identity changes aren't retroactive; a pre-change record poisons the subscription permanently — worker retry-loops every 5s).
  - Reverse replication (unfiltered pub on shard, sub on mono, copy_data=false): INSERT + UPDATE both flow back cleanly once identities match.
  - Sequences confirmed not replicated (subscriber seq last_value=1 with 600 rows) — T8 premise holds.
  - Plan deltas: T3 schema gets the composite unique index + REPLICA IDENTITY USING INDEX in bootstrap (before any pub); T5 wait-lsn uses publisher replay_lsn; T13 README deep-dive gains the replica-identity trap.
- 2026-08-23 · T2 scaffold: compose (3× postgres:17.11, wal_level=logical, healthchecks), Makefile skeleton, psycopg 3.2.10 pinned; up→healthy, clean→pristine verified
- 2026-08-23 · T3 schema+common+bootstrap: docs table, shardkey unique index + REPLICA IDENTITY USING INDEX verified on all 3 nodes via \d docs
- 2026-08-23 · T4 seed(500k/14.8s)+router+traffic: 1106 journaled acked writes in 8s, all→mono; insert path auto-freezes when shards authoritative (pre-sequence-fix safety)
- 2026-08-23 · T5 replicate.py: row-filtered pubs+subs, initial 500k sync ~7s under traffic, wait-lsn gate (publisher replay_lsn) ~120ms; counts converge
- 2026-08-23 · T6 drill-naive: 405 acked writes damaged (128 missing / 277 stale) via injected stall + ungated flip + decommission; reset-shards restores exact baseline
- 2026-08-23 · T7 gated cutover: 439ms pause, 0/246 acked writes lost under live traffic
- 2026-08-23 · T8 drill-sequence: silent global dup (shard0 id=1) + loud dup-key (shard1) + setval trap + interleaved fix (disjoint parity, 20 inserts). Design correction for T9: reverse stream must be created INSIDE the cutover gate (drop forward subs first — loop risk; gap risk otherwise)
- 2026-08-23 · T9 drill-rollback shipped, after three real bugs the chain surfaced:
  1. reverse stream must be armed INSIDE the cutover gate (post-flip writes predate a later slot; live forward subs would loop reverse rows) — cutover.py now flips streams under the gate
  2. sequence-drill trap rows poisoned the reverse sub (committed dup INSERT reverse-applied to mono = permanent PK conflict) — traps now run in ROLLED-BACK txns (logical replication ships only commits)
  3. router raced gate flips twice (op type then node computed from different snapshots) — route_write() now returns node+state from one post-gate snapshot; reset-shards also restarts shard seqs (truncate keeps them)
  4. bonus lesson: rollback must re-sync mono's sequence above shard-minted ids (the sequence trap, mirrored) — now a rollback step
  Clean bootstrap→rollback chain: cutover 0/256 lost, rollback 0/521 lost (173 inserts), zero warnings
- 2026-08-23 · T10 verify: state-aware gate (pre-cutover/post-cutover/post-rollback/unreplicated), union checksums 32 ranges, boundaries, global uniqueness, journal replay; verified in all 3 phases incl. under live traffic; VERIFY_INVERT recounts naive damage (132 missing/291 stale), exits 1 without evidence; psycopg %%-escape fix
- 2026-08-23 · T11 bench-index: 250k-row sync — 5.97s kept vs 3.67s copy + 0.76s rebuild; per-round DROP SUBSCRIPTION fix (orphaned slot)
- 2026-08-23 · T12 demo: 10 stages from pristine clean, exit 0 (bench→replicate→naive+inverted gate→reset→gated cutover→verify→sequence→rollback→verify)
- 2026-08-23 · T13 README: full deep-dive with measured numbers, cited sources, checked against Makefile targets
