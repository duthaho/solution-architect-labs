# Lab 08 — Idempotent Event Processing: The Exactly-Once Illusion

"Exactly-once delivery" does not exist. Any system built on retries over an
unreliable network delivers **at least once** or **at most once** — pick which
way you want to be wrong. And yet payment systems, ledgers, and inventory
counters run on Kafka all day without double-charging anyone. The trick is that
**exactly-once *effects* can be engineered** even though exactly-once
*delivery* cannot — and the entire trick fits in one database transaction.

This lab takes the least forgiving workload there is — account balance updates,
where `balance += x` applied twice is not an error but silent corruption — and
runs it through the full ladder: a naive consumer that corrupts ~$10k under
crashes, the one-transaction fix that survives the *identical* crash schedule
with zero drift, the outbox pattern that closes the producer-side hole, and a
poison pill that parks a partition until a DLQ (and a dedupe-safe replay)
rescues it.

```bash
make demo                 # naive vs idempotent under identical chaos (~4 min)
make drill-ghost-publish  # the dual-write hole, then the outbox closing it
make drill-poison         # pill -> stalled partition -> DLQ -> replay x2, still clean
```

---

## 1. The problem

Every event pipeline is this loop:

```
consume(msg) -> apply effect -> commit offset
```

Two of those steps mutate state in **different systems** (your DB, Kafka's
offset store), and the process can die between any two lines. Enumerate the
orderings:

```
crash after apply, before offset commit  -> redelivery -> effect applied TWICE
crash after offset commit, before apply  -> no redelivery -> effect applied NEVER
```

There is no ordering that gives you exactly-once. Commit first and you're
at-most-once (lose money). Apply first and you're at-least-once (double-charge).
This is not an implementation bug — it's the two-generals problem wearing a
consumer group id. The broker faces the same dilemma talking to the producer:
if the ack for a publish is lost, the producer must resend (duplicate) or give
up (loss). Retries are how distributed systems work, so **duplicates are not an
anomaly; they are the contract**. At-least-once is the floor you build on.

The naive consumer in this lab is not a strawman. It commits offsets manually,
*after* applying — textbook discipline. Under `make drill-doublecharge` (a
crash after every 350 events, at the worst moment) it still corrupts **every
single account**:

```
accounts corrupted       : 50
money over-applied       : $+10,432.51   (duplicate effects)
VERDICT: CORRUPTED
```

No exception was thrown. No log line was red. Lag reached zero. Only an audit
against the source of truth noticed — which is the second lesson of this lab:
non-idempotent effects fail *silently*.

## 2. Architecture

```
producer.py ──(app retries, dupes)──► Kafka: payments (3 partitions, key=account)
   │  --mode outbox: writes outbox                │
   │  table instead, relay.py publishes           ▼
   ▼                                     consumer.py (the star)
┌───────────── MySQL ─────────────┐      naive:      UPDATE balance
│ payments          (ground truth)│      idempotent: INSERT event_id
│ accounts          (the effect)  │◄──               + UPDATE balance
│ processed_events  (dedupe)      │                  in ONE transaction
│ outbox            (phase 3)     │      --dlq: parse failures -> payments.dlq
└─────────────────────────────────┘
  relay.py: outbox -> Kafka          replay.py: payments.dlq -> payments
  chaos.py: kill/restart consumer    audit.py:  expected vs actual balances
```

- The **payments table is the source of truth**: one row per payment the
  business accepted, written by the producer *before* anything is published.
  `audit.py` replays it (each unique event once) and diffs against `accounts`.
  Because amounts are strictly positive, the drift direction names the failure:
  **over = duplicate effects, under = lost events**.
- MySQL on :3310, Kafka (single-broker KRaft) on :9096 — no clashes with the
  other labs.
- Both consumer modes use **identical offset discipline** (auto-commit off,
  commit after the batch). The only difference is inside `apply()`. That's the
  point.

## 3. Deep dives

### 3.1 Where duplicates actually come from

Three independent factories, and you run all of them:

1. **Producer retries.** A publish times out, the app calls `send()` again —
   but the first one *did* land; only the ack was lost. Kafka's idempotent
   producer (`enable.idempotence=true`, on in this lab) dedupes *transport*
   retries within a producer session, but an application-level re-send is a
   brand-new record with a brand-new sequence number. `--retry-storm`
   simulates exactly those: ~5% of events are published twice.
2. **Redelivery after a crash.** Offsets not committed → the next consumer to
   own the partition re-reads everything since the last commit. The
   `--crash-every` hook forces this deterministically.
3. **Rebalances.** A consumer that misses `max.poll.interval.ms` (a slow
   batch, a GC pause) is evicted; its partitions move *while its in-flight
   batch is still being applied*; the new owner re-reads from the last commit.
   No crash required — drill 5 (manual) reproduces this with two consumers.

One more, subtler: a SIGKILL'd consumer never sends `LeaveGroup`, so the broker
keeps its partitions assigned until `session.timeout.ms` expires (45s default —
6s in this lab). That timeout **is your redelivery latency after a hard
crash** — during it the partition is owned by a corpse and nobody consumes.

### 3.2 Offset discipline bounds the window; it cannot close it

The naive consumer already does the best thing offsets can do: disable
auto-commit, apply first, commit after. That shrinks the redelivery window from
"whatever auto-commit last did" to "the current batch" — and no smaller. The
crash hook in this lab fires *after* an apply and *before* the commit,
guaranteed, every 350 events. Six crashes later the ledger is off by ten
thousand dollars.

Chase the window to its limit: commit offsets after *every single message*.
The crash can still land between the `UPDATE` and the `commit()` — two systems,
two writes, no shared transaction. You cannot fix a two-systems problem by
reordering the writes. Correctness has to move into **one** system:

```sql
BEGIN;
INSERT INTO processed_events (event_id) VALUES (?);   -- PK collision = seen it
UPDATE accounts SET balance_cents = balance_cents + ? WHERE id = ?;
COMMIT;
```

Now redelivery hits the primary key, `rowcount = 0`, skip. The dedupe check and
the effect commit **atomically** — crash before the commit and neither
happened; crash after and both did. There is no third state, so replays are
no-ops. `drill-idempotent` runs the *identical* chaos schedule: 6 restarts,
hundreds of redelivered events (`dedupe-skipped=67` in a typical run), zero
drift. Kafka offsets are demoted to what they really are: a *performance*
optimization (where to resume), not a correctness mechanism.

### 3.3 Why the dedupe key must live in the effect's transaction

The most common wrong fix in the wild: "we check Redis
`SETNX processed:{event_id}` before applying." It reintroduces the exact bug it
claims to fix, because check-then-act spans two systems again:

```
SETNX ok -> crash before DB apply    -> event marked processed, NEVER applied (loss)
DB apply -> crash before SETNX       -> redelivery, not marked, applied TWICE (dupe)
```

Same two orderings as §1, one system further left. Any dedupe store that is not
transactional with the effect — Redis, a different DB, a bloom filter in
memory, "we log it and check the log" — has this property. The rule is short:
**the dedupe write and the effect must commit or roll back together.** If your
effect lives in MySQL, the dedupe table lives in MySQL. If your effect is a
call to a third-party API that offers no transactions... that's why Stripe
takes an `Idempotency-Key` header: you *push* the dedupe into the system that
owns the effect.

Corollary: if the effect is naturally idempotent (lab 03's whole-document
upsert keyed by PK), you get all of this for free — the state itself is the
dedupe table. `balance += x` is the opposite: it has no memory of having been
applied. That's what makes ledgers the hard case.

### 3.4 The producer's half: the dual write and the outbox

The consumer being perfect doesn't save you if events never reach the topic.
The direct-mode producer does the classic dual write:

```
INSERT INTO payments ...; COMMIT;     -- system 1: the business decision
producer.produce(...)                 -- system 2: the announcement
```

`drill-ghost-publish` kills it between the two. The DB says "payment accepted";
Kafka never heard of it; there is no process left to retry. The audit finds the
money missing — and typically *more* than one event, because `produce()` is
asynchronous: everything still unacked in the client buffer dies with the
process too. No retry policy, no `acks=all`, no idempotent producer helps —
the loss happened before Kafka was ever involved.

The fix inverts the structure — write the event **into the same database, in
the same transaction** as the state, and let a relay move it to Kafka later:

```sql
BEGIN;
INSERT INTO payments ...;
INSERT INTO outbox ...;
COMMIT;                       -- the decision and the announcement are now ONE write
```

Crash anywhere: the event is either durably queued (the relay publishes it
after restart) or the transaction rolled back (the caller sees the failure).
The in-between state cannot exist. The relay itself stays dumb on purpose —
publish, *then* mark `published_at`. Crash between the two and it republishes:
duplicates, not loss, and duplicates are already handled downstream. The
system-wide invariant: **every component may duplicate; exactly one component
(the consumer's transaction) deduplicates.**

Why not the alternatives?

- **2PC / XA** between MySQL and Kafka: Kafka doesn't speak XA, and even where
  both sides do, a coordinator crash leaves participants blocked holding
  locks. The outbox gets the same atomicity with no coordinator, using the
  transaction you already have.
- **Kafka transactions / EOS**: real, but scoped. `read-process-write` where
  *both* ends are Kafka topics (Streams apps) — consume offsets and produced
  records commit atomically inside Kafka. The moment the effect leaves Kafka
  for your DB, you're back to two systems and EOS covers neither the UPDATE
  nor the ghost write. Know its boundary before an interviewer asks.
- **CDC as outbox** (lab 03 + this lab): tail the binlog of an outbox table
  with Debezium instead of polling it. Same pattern, lower latency, and the
  relay you didn't have to write.

### 3.5 Poison pills: ordering vs progress

One malformed event — a buggy deploy encoding `amount_cents` as the string
`"NaN"` — and the consumer throws at parse time. It crashes; the supervisor
restarts it (that's what Kubernetes is *for*); it re-reads the same offset and
throws again. `drill-poison` shows the result: a crash-loop and a partition
parked forever behind one bad record, lag climbing while every healthy event
behind the pill waits.

```
GROUP            TOPIC     PARTITION  CURRENT-OFFSET  LOG-END  LAG
balance-consumer payments  2          100             271      171   <- parked
```

Three options, in ascending order of adulthood:

- **Block** (default): perfect ordering, zero progress. Right answer only if
  ordering is genuinely load-bearing for correctness — and then a human gets
  paged.
- **Skip**: progress, but the event is *gone*. For a ledger, silent loss.
- **DLQ**: move the raw bytes to `payments.dlq`, commit, move on. Progress for
  the many, quarantine for the one — and the debt is still visible, countable,
  and replayable.

Two rules make a DLQ safe instead of a garbage chute. First, **only permanent
failures go to the DLQ** — deserialization and validation errors, things a
retry can never fix. A DB timeout or deadlock is transient: DLQ-ing it turns a
30-second blip into data loss; crash and retry instead (this consumer does —
compare the `except` around `parse()` with the *absence* of one around
`apply()`). Second, **replay must be idempotent**. Replay tooling is exactly
the kind of run-from-a-terminal code that double-fires — an operator reruns it,
two people run it at once. `replay.py --times 2` republishes every DLQ event
*twice* on purpose, repaired from the source of truth; the audit still comes
back clean, because the consumer's dedupe doesn't care why a duplicate exists.
Drill 2 is what makes drill 4 boring — that's the dependency to remember.

### 3.6 How long do you keep `processed_events`?

The dedupe table grows forever if you let it. You can prune it, but the moment
you delete an event_id you can no longer detect that event's duplicate — so the
retention question is really: **how late can a duplicate arrive?**

Bound it from the sources (§3.1): redelivery duplicates arrive within your
worst-case consumer outage; replayed-DLQ duplicates within your incident
response time; app-retry duplicates within seconds. The hard ceiling is Kafka
**topic retention**: a duplicate cannot be older than the log that redelivers
it. Keep dedupe rows ≥ topic retention (+ margin for DLQ replays, which
re-emit *old* event_ids into a *new* log position) and prune by `processed_at`
with a daily job. A `processed_events` row here is ~60 bytes; a year of 1k
events/sec is ~2TB — prune, partition by day, or both. And resist the
temptation to make it a bloom filter: false positives here mean *silently
dropping real payments*.

## 4. Runbook

```bash
make up install bootstrap   # mysql :3310, kafka :9096, topics, 50 accounts @ $1000

make drill-doublecharge     # naive + retry storm + 6 crashes -> ~$10k drift
make drill-idempotent       # identical schedule -> 0 drift, 2000 dedupe rows
make drill-ghost-publish    # direct: ghost + dead buffer -> loss; outbox: clean
make drill-poison           # pill -> crash loop -> lag -> DLQ -> replay x2 -> clean
make demo                   # drills 1+2 back to back + comparison table

make reset                  # fresh ledger/topics/offsets between experiments
make lag                    # consumer group lag per partition
make audit                  # ledger audit any time
make clean                  # pristine machine
```

Every drill starts with `make reset` and ends with an audit that *asserts* the
expected verdict — if a drill can't demonstrate its failure mode, it fails
loudly rather than teaching a lie.

## 5. Failure drills

| # | Drill | What it proves |
|---|-------|----------------|
| 1 | `drill-doublecharge` | At-least-once + non-idempotent effect = silent corruption, measured in dollars — with *correct* manual offset commits |
| 2 | `drill-idempotent` | Dedupe-in-the-effect's-transaction survives the identical crash schedule with zero drift |
| 3 | `drill-ghost-publish` | The dual write loses acked payments on one crash (including the unflushed producer buffer); the outbox makes the loss state unrepresentable |
| 4 | `drill-poison` | One bad record parks a partition behind a crash-looping consumer; DLQ restores progress; double-replay is safe *because* of drill 2 |
| 5 | manual: start `consume-idempotent`, then a second `scripts/consumer.py --mode idempotent` mid-drain, watch `dedupe-skipped` | Rebalances are a duplicate factory even with zero crashes — partitions move, in-flight batches replay |

## 6. Production checklist (when it's real money and a pager)

- **Assume every message arrives at least twice.** Design reviews should ask
  "what happens when this handler runs twice?" the way they ask about null.
  If the answer involves the word "unlikely", it's wrong.
- **Dedupe in the same transaction as the effect. No exceptions.** Not Redis,
  not a cache, not a check-then-act against yesterday's snapshot. If the
  effect's owner offers no transactions, push the idempotency key *into* it
  (Stripe-style) or front it with something that does.
- **Producers: outbox for anything that must not vanish.** Dual writes are
  fine for metrics and logs; they are incidents for money. Debezium's outbox
  router gives you the relay for free if you already run CDC.
- **Choose per topic: block, skip, or DLQ** — before the incident, in writing.
  DLQ only permanent failures; alert on DLQ depth > 0; treat replay as a
  rehearsed runbook (and rerun it twice in the rehearsal, to prove you can).
- **Run the audit continuously.** This lab's `audit.py` is a reconciliation
  job: source of truth vs applied effects, scheduled hourly, alerting on any
  drift. Every serious payment shop has one; the consumer being green is not
  evidence.
- **Size the dedupe window consciously**: retention ≥ Kafka topic retention +
  DLQ replay margin, pruned by date, plain unique index — never probabilistic.
- **Watch `session.timeout.ms` and `max.poll.interval.ms`**: the first is your
  redelivery latency after SIGKILL, the second is how slow a batch can be
  before the group decides you're dead and manufactures duplicates for you.
- **Load-test the dedupe hot path.** The INSERT adds a write per event on the
  effect DB. It's cheap (PK insert into a slim table) but it's not free, and
  it must scale with the topic, not with the accounts.

## 7. Interview questions

Answer without notes, out loud:

1. Why is exactly-once *delivery* impossible, while exactly-once *processing*
   is not? Where exactly does the impossibility live, and where does the
   engineering go instead?
2. A consumer commits offsets manually, after applying, never uses
   auto-commit — and still double-applies events. Walk through the crash that
   does it. What *is* offset discipline good for, then?
3. Why must the dedupe key be written in the same transaction as the effect?
   Give both failure orderings of a Redis-based dedupe check.
4. Producer writes DB then publishes to Kafka, with `acks=all`, idempotence
   on, infinite retries. What can still be lost, and when? How does the outbox
   make that state unrepresentable, and what new component (with what
   guarantee) does it require?
5. What does Kafka EOS (transactions) actually cover — and at what boundary
   does it stop helping this lab's workload?
6. One malformed event is stalling a partition. Argue block vs skip vs DLQ for
   a payments topic; what two conditions make DLQ + replay safe?
7. How long do you retain `processed_events`, and what system property puts a
   ceiling on how late a duplicate can arrive?
8. Your balances audit clean every night, and one morning they don't, by
   +$340, across 12 accounts. Duplicates or loss? What do you check, in order?

## 8. File map

```
docker-compose.yml       mysql :3310, kafka (KRaft) :9096
sql/schema.sql           accounts, payments (truth), processed_events, outbox
scripts/
  common.py              config, connections, event wire format
  bootstrap.py           tables + seed + topics; --reset for fresh drills
  producer.py            --mode direct|outbox, --retry-storm, --crash-at N
  consumer.py            THE CORE: naive vs idempotent apply, one txn dedupe,
                         manual commits, --dlq, --crash-every hook
  chaos.py               supervisor: restart on crash, cap on crash-loop
  relay.py               outbox poller -> Kafka, publish-then-mark
  inject.py              the poison pill (true amount in DB, "NaN" on the wire)
  replay.py              DLQ -> payments, repaired from truth, --times N
  audit.py               ledger audit: expected vs actual, drill gates
  demo.py                drills 1+2 + comparison table
```
