# Backlog

The raw source of problems for these labs — questions from real engineers and
interviews — curated, deduplicated against the labs that already exist, and
tagged so the next lab is an easy pick instead of a blank page.

This is a **catalog, not a queue**: a problem here is a candidate, not a
commitment. Runnable solutions live in `labs/NN-name/`; this file just decides
what's worth building and what's already covered.

## Status legend

| | Meaning |
|---|---|
| ✅ **Covered** | an existing lab already answers this (see the mapping) |
| 🎯 **Next up** | fits the repo's data-infra core; strong candidate for the next lab |
| 💡 **Candidate** | a good problem, buildable here with some framing |
| 🧭 **Different track** | valuable but off the repo's "data under traffic" identity; park for a separate track/repo unless that track grows |

## Repo identity (the filter)

The labs are about **the hard, scary problems of data infrastructure under
traffic**: migrating live data, evolving schemas, keeping systems consistent,
shipping without downtime, surviving failure. A problem earns a `labs/NN` slot
when it's *deterministically reproducible on a laptop with docker + a failure
drill*. Problems that are really about application architecture, security
posture, or raw throughput are tagged 🧭 — not lesser, just a different book.

## The catalog

| ID | Problem | Category | Status | Notes / mapping |
|----|---------|----------|--------|-----------------|
| P01 | Soft delete for many big tables (mirror `deleted` schema per table) | Lifecycle | ✅ Covered | **Lab 10**. `deleted_at` vs mirror-schema vs archiver, benchmarked. |
| P02 | Release a DB migration with no downtime (schema + query logic change together) | Migration | ✅ Covered | **Lab 09** (expand/contract) + **Lab 02** (online big-table ALTER). Backward-compat window is exactly lab 09's thesis. |
| P03 | Async replication: primary acks a write, crashes before replicating, replica promoted → lost row | HA / Replication | ✅ Covered | **Lab 04** measures this exact RPO window and shows semi-sync / fencing. |
| P04 | Concurrent money transfer: balance 50k, two devices each send 50k at once → double-spend | Concurrency | ✅ Covered | **Lab 11**. Lost update reproduced deterministically; fixed four ways (`FOR UPDATE`, version column, atomic conditional `UPDATE`, append-only ledger) + deadlock drill and conservation-of-money verifier, benchmarked under hot-account contention. |
| P05 | Top-K most-viewed products: 10M products, 10B views/day, windows per minute/day/month | Scale / Analytics | ✅ Covered | **Lab 14**. Six contenders on one seeded Zipf stream (exact oracle, CMS, CMS conservative-update, Space-Saving, Redis TOPK/HeavyKeeper on core Redis 8, MySQL per-minute rollup); width-sweep drill measures the precision cliff (recall 1.000 → 0.550), merge drill proves CMS linearity bit-identically, shows CU breaking it, and reproduces the cross-window trap (day's #3 absent from every minute's top-K); verify gate replays the stream and recomputes every invariant. Parked: skew sweep, Redis restart drill. |
| P05b | Distributed unique ID generation: short, sortable, no collision at tens of thousands tx/s | Scale / Coordination | ✅ Covered | **Lab 13**. Snowflake 41/10/12 with injectable clocks: backwards-clock drill (naive duplicates vs error/wait/hold), sequence-exhaustion drill, zombie-worker drill on MySQL-leased worker ids (SIGSTOP past TTL, id reclaimed, collision guaranteed in naive mode); benched vs UUIDv4/v7, AUTO_INCREMENT, Redis INCR + B-tree insert-locality bench. |
| P06 | "On this day" / memories feature on a social feed, RabbitMQ, must not miss messages | Messaging / Consistency | 💡 Candidate | Overlaps **Lab 08** (idempotent processing, outbox, DLQ). Could be a lab 08 extension or a fan-out-on-schedule variant rather than a fresh lab — the "no miss message" core is already covered. Decide: extend 08 or build a scheduled-fanout lab. |
| P07 | Inter-service read path: service A needs B's data; CUD via MQ, but reads? (avoid N+1 loops) | Architecture | 💡 Candidate | Data-replication angle (materialized read model via CDC) is on-theme and overlaps **Lab 03**. The pure "how do services talk" part (API composition, BFF, caching) is more architecture than data-infra. Frame narrowly as "local read replica via events" to fit. |
| P08 | Third-party API times out (5s) → thread-pool exhaustion → cascading failure across services | Resilience | 🧭 Different track | Circuit breaker / bulkhead / timeout budget / load shedding. Reproducible and drill-friendly (a deliberately slow dependency), but it's a resilience topic, not data-under-traffic. Good first lab of a **resilience track** if that track ever grows. |
| P09 | PII encryption at scale: envelope encryption (DEK/KEK), key rotation over 100M rows | Security | 🧭 Different track | Per-row DEK vs shared DEK, and "rotate KEK without re-encrypting all data" (re-wrap DEKs, not data). Genuinely interesting and has a clean drill, but it's a security-architecture topic. Park for a **security track**. |
| P10 | Password rotation policy + prevent reuse of old passwords | Security | 🧭 Different track | Hashed-history window, per-user salt, timing. Small; more of an appendix to P09's security track than a standalone data-infra lab. |
| P11 | Bulk download: 100k+ files (~500GB) → one or many zips, user clicks "Download" | Throughput / Systems | 🧭 Different track | Streaming zip, async job + progress, presigned URLs, backpressure, resumability. Great systems problem, zero data-consistency angle. Separate track. |
| P12 | Stream LLM responses token-by-token to the client like ChatGPT | Throughput / Systems | 🧭 Different track | SSE / chunked transfer, backpressure, cancellation, token buffering. On-trend but off-identity for this repo. Separate track. |
| P13 | Flash-sale inventory reservations: prevent oversell on a hot SKU at thousands of reservations/s (Shopify moved this from Redis to MySQL) | Concurrency / Migration | ✅ Covered | **Lab 12**. Oversell reproduced deterministically; fixed three ways ending in the SKIP LOCKED capped pool (composite-PK lock evidence via `data_locks`), TTL expiry sweep, plus the full Redis→MySQL shadow-mode migration with mismatch metric and gated mid-burst cutover. Source: [Shopify Engineering](https://shopify.engineering/scaling-inventory-reservations). |
| P14 | Reshard a live Postgres under traffic: split one overloaded DB into N shards with no downtime and a rollback path | Migration / Sharding | ✅ Covered | **Lab 15**. 1→2 split via row-filtered logical replication (no double-writes — Figma's argument, lived): naive mid-lag cutover reproduced (~400 acked writes lost) vs LSN-gated flip (0 lost, sub-second pause) with the reverse rollback stream armed inside the gate; sequence drill (loud dup, silent global dup, setval trap, interleaved fix — and the mirror trap on rollback); replica-identity landmine; phase-aware verify gate + inverted naive proof; drop-indexes sync bench. Parked: crash-mid-cutover resume, slot WAL-retention, long-txn stall, REPLICA IDENTITY FULL drills, PgBouncer PAUSE variant. Sources: [Notion](https://www.notion.com/blog/sharding-postgres-at-notion), [Notion re-shard](https://www.notion.com/blog/the-great-re-shard), [Figma](https://www.figma.com/blog/how-figmas-databases-team-lived-to-tell-the-scale/), [Slack/Vitess](https://slack.engineering/scaling-datastores-at-slack-with-vitess/). |
| P15 | Cache that is provably consistent with the DB: CDC-driven invalidation + shadow verification (Uber CacheFront, 40M→150M reads/s) | Caching / Consistency | 💡 Candidate | Overlaps **Lab 03**'s CDC plumbing but asks a new question: *how stale is your cache, measured?* Binlog-tailing invalidator, write-through protocol, and a shadow mode that reads cache+DB simultaneously and emits a mismatch metric (Uber: 99.99%). Drills: kill the invalidator and watch staleness grow; cache-aside vs CDC-invalidate benchmarked. Source: [Uber Engineering](https://www.uber.com/blog/how-uber-serves-over-40-million-reads-per-second-using-an-integrated-cache/). |
| P16 | Hot partition on a messages store: one huge channel melts the node, latency cascades cluster-wide (Discord, trillions of messages) | Scale / Hot keys | 💡 Candidate | The fix that generalizes is **request coalescing**: a data-service layer that collapses N concurrent identical reads into 1 DB query with consistent routing per key. Reproducible with any partitioned store + Zipfian load; drill: coalescing on/off under a stampede, measure p99 collapse. Pairs with P05's hot-key theme. Source: [Discord Engineering](https://discord.com/blog/how-discord-stores-trillions-of-messages). |
| P17 | Client retries a payment POST after a timeout — charge them once: idempotency keys with stored responses (Stripe) | Consistency / API | 💡 Candidate | Overlaps **Lab 08**'s idempotent-consumer core, so frame as an 08 extension: key + *recovery point* state machine in Postgres, replay the stored response, contend two concurrent retries on the same key (`ON CONFLICT` / row lock). Drill: crash mid-handler between external call and commit. Sources: [Stripe blog](https://stripe.com/blog/idempotency), [brandur.org implementation](https://brandur.org/idempotency-keys). |
| P18 | Count billions of usage events for money (creator payouts): dedup, never overcount, reprocessable pipeline (Canva) | Analytics / Correctness | 💡 Candidate | The money-grade cousin of P05: counts feed payouts, so the invariant is *exactness*, not approximation — event dedup rules, idempotent aggregation, late/duplicate event drills, full-recompute vs incremental. Could share a harness with P05 (same ingest, opposite accuracy contract). Source: [Canva Engineering](https://www.canva.dev/blog/engineering/scaling-to-count-billions/). |
| P19 | API rate limiter state at fleet scale: sharded + replicated Redis counters, migrated live from memcached (GitHub) | Resilience / Throughput | 🧭 Different track | Counter accuracy under replication and a live datastore swap are on-theme flavors, but the product is rate limiting — belongs with P08 in the resilience track. Source: [GitHub Engineering](https://github.blog/engineering/infrastructure/how-we-scaled-github-api-sharded-replicated-rate-limiter-redis/). |

## What to build next (recommendation)

The remaining problems in the repo's core, in rough priority order (P04
shipped as **lab 11**, P13 as **lab 12**, P05b as **lab 13**, P05 as
**lab 14**, P14 as **lab 15**):

1. **P15 / P16 / P18** are the strongest remaining candidates, all with
   published sources; P16 (hot partition + request coalescing) pairs with
   lab 14's hot-key theme, P18 is lab 14's money-grade cousin (same
   ingest, opposite accuracy contract), and P15 (provably consistent
   cache) extends lab 03's CDC plumbing with a measured-staleness
   contract.

`P06`/`P07`/`P17` are better handled by **extending labs 08 / 03** than by new
labs — worth a note so they don't get built twice.

Everything tagged 🧭 (P08–P12) is deliberately **out of scope for now**: real
problems, but building them here dilutes the repo's data-infrastructure
identity. Revisit only if a resilience / security / systems track accumulates
enough demand to stand on its own — at which point split it into its own track
or repo in one deliberate move, not one lab at a time.

## How this feeds the workflow

A problem graduates from this file to a `labs/NN` lab via the standard loop:
spec → plan → build → evidence gate. When a lab ships, flip the problem here to
✅ **Covered** and add the lab number to its Notes cell — this file stays the
single source of truth for "what's done vs what's open".
