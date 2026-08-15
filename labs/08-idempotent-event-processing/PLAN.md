# Lab 08 — Idempotent event processing at scale

> **Status: PLANNED — nothing implemented yet.**
> To continue in a fresh session, read this file top-to-bottom, then start at the
> first unchecked milestone in [Milestones](#milestones).

## The pitch

"Exactly-once" doesn't exist — but *exactly-once effects* can be engineered. This lab
takes a payment-shaped workload (account balance updates: the least forgiving,
least naturally-idempotent effect there is) and walks the full ladder:

1. **Naive consumer double-charges.** Kill it mid-batch, watch redelivery corrupt
   balances. Producer retries make it worse. Measure the damage.
2. **Consumer-side dedupe**: processed-events table updated **in the same DB
   transaction** as the effect — the only trick that actually works.
3. **Producer-side outbox**: events written transactionally with state, relayed to
   Kafka — no more "DB committed but publish failed" ghosts.
4. **Poison pill**: one malformed event blocks a partition forever. Block vs skip vs
   **DLQ + replay** — and replaying the DLQ must not double-apply (dedupe again).

`make demo` prints a before/after ledger audit: naive consumer → N corrupted balances;
idempotent consumer, same crash schedule → 0.

## Architecture

```
producer.py ──(retries, dupes)──► Kafka topic: payments (3 partitions)
   │  Phase 3: writes outbox table instead        │
   ▼                                              ▼
┌──────── MySQL ────────┐              consumer.py (the star)
│ accounts               │◄─────────── phase 1: naive UPDATE
│ processed_events (dedupe)            phase 2: effect + dedupe + offset in ONE txn
│ outbox                 │             phase 4: try/except → dlq topic
└────────────────────────┘   relay.py (outbox → Kafka)   replay.py (dlq → payments)
```

- **Kafka single-broker KRaft** + **MySQL** (mirror lab 03's compose patterns; fresh
  ports to avoid clashes: MySQL 3310, Kafka 9096).
- `audit.py` = ground truth: expected balance per account computed from the *logical*
  event stream (each unique event applied once) vs actual `accounts` table. Any diff is
  corruption. This is the lab's `verify.py`.
- Consumer commits Kafka offsets **manually, after** the DB transaction — and the plan's
  key insight to demonstrate: offsets in Kafka are at-least-once no matter what, so
  correctness must live in the DB transaction (dedupe), not in offset discipline.
- Poison pill = event with `amount: "NaN"` injected by `inject.py`.

## File tree (target)

```
labs/08-idempotent-event-processing/
├── README.md / PLAN.md / docker-compose.yml / Makefile / requirements.txt
├── sql/schema.sql          # accounts, processed_events(event_id PK), outbox
└── scripts/
    ├── common.py           # conns, kafka helpers, event schema (event_id = uuid)
    ├── bootstrap.py        # topics (payments, payments.dlq), tables, seed accounts
    ├── producer.py         # --mode direct|outbox, --retry-storm flag (dup injection)
    ├── relay.py            # outbox poller → Kafka (itself idempotent via event_id)
    ├── consumer.py         # --mode naive|idempotent, --dlq on|off, crash hooks
    ├── chaos.py            # kill/restart consumer on a schedule (SIGKILL mid-batch)
    ├── inject.py           # poison pill injector
    ├── replay.py           # drain DLQ back into payments
    └── audit.py            # logical-stream vs DB balances; duplicate/loss report
```

## Makefile targets

```
up / down / clean / install / bootstrap
produce / consume-naive / consume-idempotent   (background, pid+log like lab 02)
chaos-start / chaos-stop
drill-doublecharge   # naive consumer + chaos → audit shows corrupted balances
drill-idempotent     # same chaos schedule → audit clean
drill-ghost-publish  # direct producer killed between DB write and publish → lost event;
                     # rerun with outbox → relay delivers after restart
drill-poison         # inject pill → partition stalls (show lag) → enable DLQ → replay
demo                 # doublecharge vs idempotent comparison table
```

## Failure drills

| # | Drill | What it proves |
|---|-------|----------------|
| 1 | `drill-doublecharge` | At-least-once + non-idempotent effect = corruption; measured, not asserted |
| 2 | `drill-idempotent` | Dedupe-in-same-txn survives the identical crash schedule with 0 diffs |
| 3 | `drill-ghost-publish` | Dual-write (DB then publish) loses events on crash; outbox closes it |
| 4 | `drill-poison` | A single bad event halts a partition (consumer lag graph); DLQ restores progress; replay is safe **because** of drill 2's dedupe |
| 5 | manual: run two consumers in the same group, trigger rebalance mid-batch | Rebalance is a duplicate factory even without crashes |

## Interview questions (README)

Why "exactly-once delivery" is impossible but exactly-once processing isn't; where the
dedupe key must live (same txn as the effect — why Redis-based dedupe is wrong); outbox
vs 2PC vs Kafka transactions (and what Kafka EOS actually covers); ordering vs progress
trade-off for DLQ; how long to retain processed_events (dedupe window vs Kafka
retention).

## Milestones

- [ ] **M1 — Infra**: compose (Kafka KRaft + MySQL), bootstrap, schema, seed accounts.
      Gate: `make up bootstrap` idempotent; topic + tables exist.
- [ ] **M2 — Naive pipeline + audit**: producer (direct), naive consumer, `audit.py`,
      `chaos.py`. Gate: `make drill-doublecharge` shows nonzero corruption.
- [ ] **M3 — Idempotent consumer**: dedupe table in same txn, manual offset commit.
      Gate: `make drill-idempotent` → 0 diffs under identical chaos schedule.
- [ ] **M4 — Outbox**: producer `--mode outbox`, `relay.py`, ghost-publish drill.
      Gate: `make drill-ghost-publish` shows loss (direct) vs none (outbox).
- [ ] **M5 — Poison pill + DLQ**: inject, lag display, DLQ mode, `replay.py`.
      Gate: `make drill-poison` end-to-end, replay causes no double-effects.
- [ ] **M6 — Polish**: `make demo` comparison table, README deep-dive, root README
      row → ✅ Ready, `make clean` pristine, trim PLAN.md.
