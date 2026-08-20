# Lab 12 — Flash-sale inventory reservations (and the Redis→MySQL migration)

Based on [Shopify Engineering — "We replaced Redis with MySQL for inventory
reservations — and it scaled"](https://shopify.engineering/scaling-inventory-reservations).

## 1. The problem

A flash sale points thousands of concurrent "reserve 1 unit" requests at one
hot SKU. The naive handler — read the counter, check availability, write the
incremented value — **oversells**: two workers read the same `reserved`
value, both see room, both write `reserved + 1`, and one increment silently
vanishes while *both* customers hold a reservation. In this lab, 96 requests
against a capacity of 50 produce **96 successful reservations** and a counter
that claims only 12 units are held.

Two hard problems live here, and Shopify's post covers both:

1. **Correctness under contention** — prevent oversell without serializing
   the entire sale onto one row lock (the cure that kills throughput).
2. **Changing the engine mid-flight** — their reservations lived in Redis;
   moving them into MySQL happened via *shadow mode*: dual-write both stores,
   measure mismatches, and cut over only when the metric reads zero.

Everything here is deterministic and asserted by exit codes: the bug
reproduces every run, the fixes pass an invariant gate every run.

## 2. Architecture

```
                 burst: 8 workers x 12 rounds (demand 96 > capacity 50)
                                      |
              phase.txt: redis -> shadow -> mysql   (the migration flag)
                                      |
        +--------------------- reservation client ---------------------+
        |                             |                                |
   phase=redis                  phase=shadow                     phase=mysql
        |                             |                                |
  Redis (legacy)            Redis (truth) + MySQL              MySQL only
  Lua DECR-with-floor       dual-write, comparator             MODE=naive|a|b|c
        |                             |                                |
        +----------- verify.py: the invariant gate (exit code) --------+
          never oversold - slots conserved - exactly-once - stores agree
```

MySQL strategies (MODE):

| MODE | Mechanism | Verdict |
|------|-----------|---------|
| `naive` | read → check → write computed value | oversells (the bug) |
| `a` | `SELECT … FOR UPDATE` on the counter row | correct; hot-row serialization |
| `b` | `UPDATE … WHERE reserved + sold < capacity` | correct; lock held per-statement |
| `c` | capped pool of slot rows, `FOR UPDATE SKIP LOCKED` | correct; contention spread across rows (Shopify) |

## 3. Deep dive

### The capped pool (strategy c)

Instead of one counter row, capacity is materialized as `capacity` claimable
rows in `slots`. A reservation claims one:

```sql
SELECT item_id, slot_id FROM slots
WHERE item_id = ? AND state = 'free'
LIMIT 1 FOR UPDATE SKIP LOCKED;
```

`SKIP LOCKED` is the trick: a worker that finds a row locked by someone else
*skips it* instead of queueing on it, so N workers claim N different slots in
parallel. The bench below shows what that buys. When the pool looks empty,
a **single-flight replenish** (`GET_LOCK`) frees slots held by expired
reservations while contenders wait — the thundering-herd guard from the
Shopify design — then the claim is retried once before a clean `sold_out`.

### Why the composite primary key

`slots` uses `PRIMARY KEY (item_id, slot_id)` — the columns the claim filters
on are the PK prefix. Shopify's observation, reproduced by `make locks`: a
claim that searches via a secondary index holds **two** record locks (the
secondary-index record plus the clustered record it points at); a claim that
searches via the PK prefix holds **one**. Half the lock-manager work on the
hottest path in the system.

### Reservation lifecycle

`active → committed` (buyer pays) or `active → expired` (TTL passes, sweep
returns the capacity). The sweep is idempotent: it counts overdue `active`
rows, returns counter-mode capacity, frees pool slots, and flips the states
in one transaction. `make drill-expiry` exhausts the pool with 1-second TTLs,
sweeps, and proves exactly `capacity` fresh reservations succeed afterwards.

### The shadow migration

The legacy store is a Redis counter behind an atomic Lua script. Note that
it is **correct** — the motive for leaving is that a bare counter is
unjoinable and unauditable next to relational data, which matches Shopify's
actual reasoning (observability, not Redis bugs).

- **Shadow mode**: Redis stays the source of truth; every reservation is
  also written to MySQL, idempotently keyed by `reservation_id`. A
  comparator diffs the two stores' reservation sets — the *mismatch metric*.
  Failed shadow writes are journaled and counted, never hidden.
  `make drill-shadow` first shows a clean pass (0 mismatches), then
  *deliberately injects divergence* and proves the metric catches exactly
  the injected amount — a metric you haven't seen fail is not evidence.
- **Cutover**: `make drill-cutover` flips the source of truth **in the
  middle of a live burst**. The flip is gated: the world pauses at a round
  barrier, the comparator must read 0 mismatches, then the phase flag flips
  and workers proceed against MySQL. Because dual-write kept MySQL current,
  the flip transfers no state. The gate: 48 reservations acked in the Redis
  era + 2 in the MySQL era = exactly capacity, verified across both stores.

### The invariant gate

`verify.py` joins three sources of truth — the seed baseline, the client
journals (what was *acknowledged*), and the stores — and asserts: never
oversold; slot conservation (`free + claimed == capacity`, claimed ==
active+committed); expired reservations hold no slot; exactly-once per
acknowledged reservation in the owning store; dual-written acks present in
MySQL; Redis conservation. `make verify-naive` inverts the exit code and
**must pass after the naive run** — proof the checker catches the bug rather
than vacuously passing.

## 4. Runbook

```bash
make demo          # the whole story end-to-end (exit 0 = every gate passed)

# or step by step:
make up install bootstrap seed
MODE=naive make drill-burst   # exit 0 = oversell reproduced
make verify-naive             # exit 0 = the gate CAUGHT it
make seed && MODE=c make drill-burst && make verify
make seed && make drill-expiry && make verify
make seed && make locks       # 1 record lock (composite PK) vs 2 (secondary)
make seed && make drill-shadow && make verify
make seed && make drill-cutover && make verify
make bench
make clean         # pristine machine
```

Bench on a laptop (8 workers × 40 rounds, capacity == demand == 320):

```
strategy                ops/s   p50 ms   p95 ms  retries  rejects  oversold
a FOR UPDATE row          126    31.57    58.64        0        0         0
b atomic UPDATE           129    30.67    58.47        0        0         0
c SKIP LOCKED pool        304    16.83    22.26        0        0         0
```

Strategy c: ~2.3× the throughput, p95 nearly a third — same machine, same
workload, the only difference is where the contention lands.

## 5. Production checklist — what changes with real money and a pager

- **Pool sizing**: Shopify caps the pool and replenishes inline; size it for
  peak claim rate, not total inventory. A pool of millions of rows for a
  cold item is waste; a pool of 10 for a drop is a queue.
- **Replenish under the herd**: the single-flight lock is load-bearing.
  Without it, every rejected claimer runs the replenish scan simultaneously.
- **TTL sweep cadence** vs reservation length: sweep too rarely and sold-out
  rejections lie; too often and you burn the hot index. Shopify replenishes
  inline on exhaustion — the sweep is the backstop, not the path.
- **Shadow mode duration**: run it through a real peak before trusting the
  metric. A comparator that has only seen quiet traffic proves nothing.
- **Cutover rollback**: this lab cuts over forward-only. In production keep
  the dual-write running *reversed* (MySQL truth, Redis shadow) for a
  rollback window.
- **Idempotency keys end-to-end**: the client-generated `reservation_id` is
  what makes retries, dual-writes, and the comparator all safe. Server-side
  generated IDs would break every one of those.
- **Watch `data_locks` in anger**: the composite-PK observation was found by
  looking at lock telemetry under load, not by reading docs.

## 6. Interview questions to answer without notes

1. Two devices reserve the last unit simultaneously and both succeed. Walk
   through the exact interleaving that caused it.
2. Why does `SELECT … FOR UPDATE` fix oversell but hurt a flash sale
   specifically? What's the p95 story?
3. What does `SKIP LOCKED` change about lock waits, and what anomaly does it
   deliberately accept in exchange?
4. Why did Shopify make the filtered columns the *primary key* of the
   reservations pool table? What did it do to locks held per claim?
5. A reservation pool is exhausted mid-sale. Design the replenish path so a
   thousand concurrent rejections don't stampede it.
6. Reservations must expire. Compare inline expiry-on-claim vs a background
   sweep — failure modes of each?
7. You're migrating a live reservation system between datastores. Why
   shadow-mode dual-write instead of stop-the-world copy or double-read?
8. What exactly does the mismatch metric measure, and why must you see it
   *fail* before you trust it?
9. When is it safe to flip the source of truth, and what makes the flip
   transfer zero state?
10. The old store (Redis) was never wrong. Argue the migration anyway — what
    was the actual cost of keeping reservations outside the relational store?

## 7. File map

| File | What it does |
|------|--------------|
| `docker-compose.yml` | MySQL 8.0.43 (:3318), Redis 7.4 (:6390), optional adminer (:8092, profile `ui`) |
| `Makefile` | every drill and gate; `make demo` is the whole story |
| `sql/schema.sql` | `items` counter, `reservations` journal/lifecycle, `slots` capped pool (composite PK) |
| `scripts/common.py` | env config, MySQL/Redis connections, journals, phase flag |
| `scripts/bootstrap.py` | applies the schema |
| `scripts/seed.py` | resets both stores to one item at CAPACITY, phase=redis |
| `scripts/strategies.py` | the four MySQL reservation strategies + single-flight replenish |
| `scripts/drill_burst.py` | the flash-sale burst (barrier-synced, deterministic), exit-code contract |
| `scripts/sweep.py` | idempotent TTL expiry sweep |
| `scripts/drill_expiry.py` | exhaust → sweep → reclaim, exit-code asserted |
| `scripts/locks.py` | `performance_schema.data_locks` evidence: composite PK vs secondary index |
| `scripts/redis_store.py` | legacy Lua reserve (atomic, correct) + the store comparator |
| `scripts/drill_shadow.py` | dual-write shadow mode; mismatch metric proven non-vacuous |
| `scripts/drill_cutover.py` | mid-burst source-of-truth flip, gated on 0 mismatches |
| `scripts/bench.py` | a vs b vs c under identical contention |
| `scripts/verify.py` | the invariant gate; `VERIFY_INVERT=1` for the naive proof |
