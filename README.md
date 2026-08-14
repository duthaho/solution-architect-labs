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

| # | Lab | Problem | Status |
|---|-----|---------|--------|
| 01 | [Elasticsearch zero-downtime reindex](labs/01-es-zero-downtime-reindex/) | Change the mapping of a large live index with continuous writes, zero read downtime, and zero data loss | ✅ Ready |
| 02 | [MySQL online migration of a big table](labs/02-mysql-online-migration/) | ALTER a 100M+ row table under traffic (gh-ost-style: chunked backfill, binlog catch-up, atomic cutover) | ✅ Ready |
| 03 | [Sync big data between two datasources](labs/03-cdc-mysql-to-es/) | CDC with Debezium/Kafka: MySQL → Elasticsearch, ordering, delivery guarantees, reconciliation | ✅ Ready |
| 04 | Deploy with no data gap and no downtime | Expand/contract schema migrations, blue-green + rolling deploys, backward compatibility windows | 🔜 Planned |
| 05 | Idempotent event processing at scale | Exactly-once illusion: dedupe keys, outbox pattern, consumer offsets | 🔜 Planned |

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
