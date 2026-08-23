# Solution Architect Labs

Hands-on labs for the hard, scary problems that define senior/staff-level engineering:
migrating live data, evolving schemas under traffic, keeping two systems consistent,
and shipping without downtime.

Each lab is **not a toy demo**. Every lab ships with:

- **A full working solution** — runs locally with `docker compose`, one `make demo` away.
- **A deep-dive explanation** — why the naive approach fails, what the real trade-offs
  are, and how the pros do it in production.
- **A step-by-step runbook** — the exact commands, in order, with expected output.
- **A production checklist** — what changes when it's 500M docs and a pager, not a laptop.
- **Failure drills** — break it on purpose, practice the rollback.

## Labs

| # | Lab | Category | Problem | Status |
|---|-----|----------|---------|--------|
| 01 | [Elasticsearch zero-downtime reindex](labs/01-es-zero-downtime-reindex/) | Migration | Change the mapping of a large live index with continuous writes, zero read downtime, and zero data loss | ✅ Ready |
| 02 | [MySQL online migration of a big table](labs/02-mysql-online-migration/) | Migration | ALTER a 100M+ row table under traffic (gh-ost-style: chunked backfill, binlog catch-up, atomic cutover) | ✅ Ready |
| 03 | [Sync big data between two datasources](labs/03-cdc-mysql-to-es/) | Consistency | CDC with Debezium/Kafka: MySQL → Elasticsearch, ordering, delivery guarantees, reconciliation | ✅ Ready |
| 04 | [MySQL failover drill: measure the data-loss window](labs/04-mysql-failover-drill/) | HA / Replication | Kill the primary under live writes, promote a replica by GTID, measure acked-but-lost rows (RPO), semi-sync, fencing, split-brain | ✅ Ready |
| 05 | [Distributed locks are a lie: fencing tokens](labs/05-fencing-tokens/) | Coordination | Reproduce Kleppmann's lock-corruption scenario deterministically (SIGSTOP past TTL), fix with storage-enforced fencing tokens | ✅ Ready |
| 06 | [Live resharding under traffic](labs/06-live-resharding/) | Migration | Split one big table into N shards with zero downtime: double-write, per-shard backfill, shadow reads, atomic cutover, rollback | ✅ Ready |
| 07 | [Cache consistency: the stale-set race](labs/07-cache-consistency/) | Consistency | Deterministically reproduce cache-aside races and thundering herd; compare TTL vs delete-on-write vs versioned keys vs CDC invalidation | ✅ Ready |
| 08 | [Idempotent event processing at scale](labs/08-idempotent-event-processing/) | Consistency | Exactly-once illusion: dedupe-in-txn, outbox pattern, poison pills, DLQ + safe replay | ✅ Ready |
| 09 | [Deploy with no data gap and no downtime](labs/09-expand-contract-deploy/) | Migration | Expand/contract schema migrations, blue-green + rolling deploys, backward compatibility windows — the series capstone | ✅ Ready |
| 10 | [Soft delete for many big tables](labs/10-soft-delete-big-tables/) | Lifecycle | `deleted_at` vs mirror deleted-schema vs background archiver, benchmarked under traffic: broken uniques, schema drift, kill-safe batched archiving, retention purge | ✅ Ready |
| 11 | [Concurrent money transfer: the double-spend](labs/11-concurrent-transfers/) | Concurrency | Reproduce the lost update deterministically (transactions don't save you), fix it four ways — FOR UPDATE, version column, atomic conditional UPDATE, append-only ledger — deadlock drill, conservation-of-money verifier, contention bench | ✅ Ready |
| 12 | [Flash-sale inventory reservations](labs/12-inventory-reservations/) | Concurrency / Migration | Reproduce oversell on a hot SKU deterministically, fix it three ways ending in Shopify's SKIP LOCKED capped pool (composite-PK lock evidence included), TTL expiry, then migrate the store Redis→MySQL live: shadow dual-write, mismatch metric, gated mid-burst cutover | ✅ Ready |
| 13 | [Distributed unique ID generation](labs/13-unique-id-generation/) | Scale / Coordination | Snowflake-style IDs with every failure drilled: backwards clock (naive duplicates vs error/wait/hold policies), sequence exhaustion, zombie worker with a leased worker-id reclaimed mid-pause — plus Snowflake vs UUIDv4/v7 vs AUTO_INCREMENT vs Redis INCR benched, including B-tree insert locality | ✅ Ready |
| 14 | [Top-K most-viewed products](labs/14-top-k-heavy-hitters/) | Scale / Analytics | Heavy hitters with bounded error, measured: exact oracle vs CMS (+conservative update) vs Space-Saving vs Redis TOPK (HeavyKeeper) vs MySQL rollup; the width-sweep precision cliff, bit-identical sketch merging (and where CU breaks it), the cross-window trap — a day's #3 that no minute's top-K contains — and a six-way bench | ✅ Ready |

New problem ideas live in **[BACKLOG.md](BACKLOG.md)** — a curated, deduplicated
catalog with categories and status, feeding future labs.

## How to use these labs

1. Read the lab's `README.md` top-to-bottom once **before** touching the keyboard —
   the value is in understanding *why*, not in pasting commands.
2. Run the happy path (`make demo`).
3. Run the failure drills. Rollback until it's boring.
4. Answer the "interview questions" section at the end of each lab without notes.

## Conventions

- Every lab is self-contained under `labs/NN-name/` with its own `docker-compose.yml`,
  `Makefile`, and `README.md`.
- `make demo` always runs the full scenario end-to-end.
- `make clean` always returns your machine to a pristine state.
- Code is Python 3.11+, dependency-pinned per lab in `requirements.txt`.

## Requirements

- Docker + Docker Compose v2
- Python 3.11+
- `make`
