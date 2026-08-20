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
| P05 | Top-K most-viewed products: 10M products, 10B views/day, windows per minute/day/month | Scale / Analytics | 🎯 Next up | Fits well. Count-Min Sketch + heap for approximate top-K, time-bucketed rollups, hot-key skew. Drill: exact vs approximate error under Zipfian traffic. Pairs naturally with a streaming ingest. |
| P05b | Distributed unique ID generation: short, sortable, no collision at tens of thousands tx/s | Scale / Coordination | 🎯 Next up | Snowflake-style: clock bits + worker id + sequence. Drills: clock skew / backwards-clock, worker-id collision, sequence exhaustion within a ms. Compare vs UUIDv7, DB auto-increment, Redis INCR. Small, self-contained, high interview value. |
| P06 | "On this day" / memories feature on a social feed, RabbitMQ, must not miss messages | Messaging / Consistency | 💡 Candidate | Overlaps **Lab 08** (idempotent processing, outbox, DLQ). Could be a lab 08 extension or a fan-out-on-schedule variant rather than a fresh lab — the "no miss message" core is already covered. Decide: extend 08 or build a scheduled-fanout lab. |
| P07 | Inter-service read path: service A needs B's data; CUD via MQ, but reads? (avoid N+1 loops) | Architecture | 💡 Candidate | Data-replication angle (materialized read model via CDC) is on-theme and overlaps **Lab 03**. The pure "how do services talk" part (API composition, BFF, caching) is more architecture than data-infra. Frame narrowly as "local read replica via events" to fit. |
| P08 | Third-party API times out (5s) → thread-pool exhaustion → cascading failure across services | Resilience | 🧭 Different track | Circuit breaker / bulkhead / timeout budget / load shedding. Reproducible and drill-friendly (a deliberately slow dependency), but it's a resilience topic, not data-under-traffic. Good first lab of a **resilience track** if that track ever grows. |
| P09 | PII encryption at scale: envelope encryption (DEK/KEK), key rotation over 100M rows | Security | 🧭 Different track | Per-row DEK vs shared DEK, and "rotate KEK without re-encrypting all data" (re-wrap DEKs, not data). Genuinely interesting and has a clean drill, but it's a security-architecture topic. Park for a **security track**. |
| P10 | Password rotation policy + prevent reuse of old passwords | Security | 🧭 Different track | Hashed-history window, per-user salt, timing. Small; more of an appendix to P09's security track than a standalone data-infra lab. |
| P11 | Bulk download: 100k+ files (~500GB) → one or many zips, user clicks "Download" | Throughput / Systems | 🧭 Different track | Streaming zip, async job + progress, presigned URLs, backpressure, resumability. Great systems problem, zero data-consistency angle. Separate track. |
| P12 | Stream LLM responses token-by-token to the client like ChatGPT | Throughput / Systems | 🧭 Different track | SSE / chunked transfer, backpressure, cancellation, token buffering. On-trend but off-identity for this repo. Separate track. |

## What to build next (recommendation)

Two problems sit squarely in the repo's core and would extend the series
cleanly, in rough priority order (P04 shipped as **lab 11**):

1. **P05b — unique ID generation.** Small, self-contained, sharp failure
   drills (clock skew, worker collision). Good "one afternoon" lab.
2. **P05 — Top-K at scale.** Bigger; introduces approximate data structures
   and streaming ingest, a new flavor for the repo.

`P06`/`P07` are better handled by **extending labs 08 / 03** than by new labs —
worth a note so they don't get built twice.

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
