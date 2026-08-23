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
