# Lab 13 — Distributed unique ID generation (Snowflake, and everything that goes wrong)

## 1. The problem

You need IDs at tens of thousands per second, across many workers, that are
**short** (they'll be primary keys), **sortable** (so inserts land at the
right edge of the B-tree and "order by id" means "order by time"), and
**never collide**. `AUTO_INCREMENT` gives you all three — through one
database's throat. UUIDv4 removes the coordination and gives up sortability
and half of your index locality. The classic answer is Twitter's Snowflake:

```
 63           22          12           0
  +------------+-----------+-----------+
  | 41 bits    | 10 bits   | 12 bits   |
  | ms since   | worker id | sequence  |
  | custom     | (0..1023) | (0..4095) |
  | epoch      |           |           |
  +------------+-----------+-----------+
```

One 64-bit integer: milliseconds since a custom epoch, a worker id, and a
per-millisecond sequence. No coordination on the hot path, k-sortable by
construction, 4096 ids/worker/ms. And **three sharp edges**, each of which
silently produces duplicate IDs in the naive implementation:

1. **The clock runs backwards** (NTP step, VM migration) → the generator
   re-traverses (timestamp, sequence) pairs it already issued.
2. **The sequence exhausts** — more than 4096 draws in one ms → a wrapping
   counter re-issues the same pairs *in the same millisecond*.
3. **The worker id is reused while its previous holder still runs** — a
   stalled process (GC pause, SIGSTOP) loses its id to a newcomer, wakes up,
   and keeps generating with it.

This lab reproduces all three **deterministically**, fixes each, and then
measures Snowflake against UUIDv4, UUIDv7, `AUTO_INCREMENT`, and Redis
`INCR` — including what your PK choice does to a MySQL B-tree.

## 2. Architecture

```
                injectable clocks (the determinism trick)
      ScriptedClock (step list) - OffsetClock (real time + offset)
                              |
        +---------------------+----------------------+
        |                     |                      |
  drill_clock            drill_exhaustion       drill_zombie
  backwards jump vs      >4096 draws in         SIGSTOP past TTL,
  naive|error|wait|hold  one logical ms         worker id reclaimed
        |                     |                      |
        +----------> ids_*.jsonl journals <----------+
                              |                lease_events.jsonl
                              v                      |
                    verify.py: the invariant gate <--+
        unique - monotonic per worker - decodable - leases exclusive
                 (VERIFY_INVERT=1 must catch the naive runs)

  worker ids: MySQL worker_leases (owner token, TTL, heartbeat)
  a generator emits only while  now < expires_at - margin  (local check)
```

## 3. Deep dive

### Why the naive generator is wrong three ways

The naive loop — `ts = clock(); seq = (seq+1) & 4095 if ts == last else 0` —
trusts two things it must not trust: that the clock is monotonic, and that
4096 is more than anyone asks for in a millisecond. Both break in
production, and both break *silently*: nothing throws, the IDs just repeat.
In this lab the clock drill produces exactly 3 duplicates and the
exhaustion drill exactly 904 — every run, because the clocks are injected.

### Backwards-clock policies (drill 2)

| Policy | On `now < last_ts` | Trade-off |
|--------|--------------------|-----------|
| `naive` | trust the clock | duplicates, silently |
| `error` | raise immediately | correct; an NTP step becomes an outage |
| `wait` | sleep it out (bounded by `WAIT_MAX_MS`) | correct; p99 absorbs the skew — fine for 200 ms, fatal for 2 h |
| `hold` | keep issuing at `last_ts` from the remaining sequence, spill forward when it exhausts | correct; no latency, but "id time" drifts from wall time until the clock catches up |

`hold` is the interesting one: it treats the 12 sequence bits as a buffer
against small regressions. The drill shows it emitting through a 5-draw
regression without waiting a microsecond, output still strictly monotonic.

### Sequence exhaustion (drill 3)

The hardened generator never wraps: at `seq == 4095` it **spins until the
clock yields a new millisecond** and continues at `(ts+1, 0)`. That converts
overload into a bounded stall (at most 1 ms) instead of corruption — the
same shape as lab 12's "reject, don't oversell". 4096/ms is also your real
capacity number: one worker tops out at ~4M ids/s; past that you add
workers, not bits.

### Worker-ID leases, and why reuse is safe (drill 4)

Worker ids come from a MySQL table, not from config: a claimant takes a
free-or-expired row with an owner token and a TTL, then heartbeats. Two
rules make reuse safe:

- **The margin.** A holder may emit only while
  `now < expires_at - LEASE_MARGIN_MS`; the row becomes claimable at
  `expires_at`. The margin between the two guarantees the old holder went
  silent *before* the new one can start — the same reasoning as lab 05's
  fencing tokens, but enforced by time instead of by the storage.
- **Local validity.** The check is one comparison against a cached
  timestamp — no DB round-trip per ID. Only heartbeats touch the DB, and a
  heartbeat that matches 0 rows means *lost*, never *retry*.

The zombie drill proves both directions. Unguarded (`ZOMBIE_NAIVE=1`): A is
SIGSTOPped past its TTL, B claims A's worker id, A resumes and keeps
emitting — **50 duplicates, guaranteed**, because both processes draw from
the same scripted logical clock and therefore traverse identical
(ts, worker, seq) triples. Guarded: A's first post-resume draw finds the
validity window closed and refuses; ids reused, zero duplicates.

### The invariant gate

`verify.py` reads every hardened journal plus the lease events and asserts:
global uniqueness, per-worker strict monotonicity, `decode(id)` matches
what the journal claims, and no two owners' lease windows overlap for the
same worker id. `VERIFY_INVERT=1` inspects **only** the naive journals and
must find violations — a gate you haven't seen fail is not evidence
(lab 11/12's rule). The gate refuses to pass on an empty set.

### What the bench actually measures

Sortability is reported as **normalized rank displacement**: merge all
workers' ids in arrival order, then measure how far each id's sorted
position is from its arrival position (as a fraction of stream length).
Random keys score ~33% — a new insert belongs *anywhere* in the keyspace.
Time-ordered keys score ~0% — a new insert belongs near the end. That, not
"adjacent inversions", is the B-tree locality story: UUIDv7's random bits
shuffle ids *within* a millisecond (which is harmless — same leaf
neighborhood) while UUIDv4 scatters them across the whole index (which is
not).

## 4. Runbook

```bash
make demo          # the whole story end-to-end (exit 0 = every gate passed)

# or step by step:
make up install bootstrap
make selftest            # layout round-trip; SELFTEST_BREAK=1 proves it can fail
make drill-clock         # backwards jump: naive dupes, three policies hold
make drill-exhaustion    # 5000 draws in one ms: naive wraps, hardened spins
make verify-naive        # exit 0 = the gate CAUGHT the naive journals
make drill-zombie-naive  # reclaimed worker id, unguarded: 50 dupes, guaranteed
make drill-zombie        # same scenario, leased: id reused, zero dupes
make verify              # the invariant gate over everything hardened
make bench               # five schemes, four processes each
make bench-btree         # the same ids as MySQL primary keys
make clean               # pristine machine
```

Captured on a laptop (your absolute numbers will differ; the shape won't):

```
policy     ids  dupes  errors  waited_ms        generator     ids  dupes  monotonic
naive       12      3       0        0.0        naive        5000    904          -
error        9      0       1        0.0        hardened     5000      0       True
wait        60      0       0      199.1
hold        16      0       0        0.0

scheme          ids     ops/s   p50 µs   p95 µs  bits  text  disp%  coordination
snowflake     80000    298894     1.20     1.36    63    18   0.03  MySQL lease per worker
uuid7         80000    152264     4.32     8.28   128    36   0.10  none
uuid4         80000    120171     3.56     6.28   128    36  33.43  none
autoinc        6000       481  7876.60 11624.20    64     4   0.01  DB round-trip per id
redis_incr    20000      7503   339.52  1487.60    64     5   0.01  Redis round-trip per id

table              rows   rows/s  file MB
pk_uuid4         200000    24190     46.1
pk_uuid7         200000    25277     46.1
pk_snowflake     200000    42198     28.3
```

Read the three tables together: the round-trip schemes are 2–3 orders of
magnitude slower per id (that's the cost of central coordination);
snowflake is the only scheme that is simultaneously fast, small, and
sorted. In the B-tree bench, snowflake's win is key width *and* locality
(~1.7× insert rate, 60% the index size); uuid4 vs uuid7 shows locality
alone — and that gap **grows** with table size, because this 200k-row index
still fits in the buffer pool. Fragmentation really bites when random
inserts start faulting cold pages.

## 5. Production checklist — what changes with a pager

- **Epoch is forever.** 41 bits of ms ≈ 69 years from *your chosen epoch*.
  Pick it once, document it, never move it — decoded timestamps silently
  shift otherwise.
- **Pick the clock policy per system, not per library default.** `error`
  for money paths (refuse over repeat), `wait` where skew is bounded by
  disciplined NTP, `hold` where latency matters more than id-time accuracy.
- **The margin is load-bearing.** `LEASE_MARGIN_MS` must exceed your worst
  heartbeat jitter + clock skew between workers and the DB. Too small and a
  paused worker can straddle the handoff — the exact bug the zombie drill
  reproduces.
- **A lost heartbeat is a stop, not a retry.** 0 matched rows means someone
  else may already hold your id. The only safe continuation is a fresh
  claim (a *new* id), never "try the heartbeat again".
- **Watch sequence saturation.** `seq` hitting 4095 regularly means you're
  at a worker's ceiling; the fix is more workers (you have 1024 slots), and
  the metric to alarm on is spin frequency, not ops/s.
- **Don't outsource the worker id to the orchestrator.** Pod ordinals and
  IPs get reused faster than leases expire. The lease table *is* the
  source of truth; everything else is a cache.
- **UUIDv7 is the right "no infrastructure" fallback.** If you can afford
  128-bit keys and don't need compact decimal ids, it buys the locality
  without the lease machinery — that's the honest trade the bench shows.

## 6. Interview questions to answer without notes

1. Walk through the bit layout of a Snowflake id. Why timestamp in the top
   bits? What breaks if worker id and sequence swap places?
2. Your generator's clock steps back 300 ms. Enumerate what `error`,
   `wait`, and `hold` each do, and pick one for a payments system.
3. Why does a wrapping sequence counter produce duplicates *silently*?
   What's the correct behavior at `seq == 4095`, and what does it cost?
4. Two processes hold the same worker id for one overlapping second. What
   is the probability they collide? (Hint: it's not "low".)
5. Design worker-id assignment for 400 pods on Kubernetes. Why is the pod
   ordinal not enough? What does your lease TTL/margin trade off?
6. A worker resumes from a 30 s GC pause. What must its very next id draw
   do, and what state makes that check O(1)?
7. Why do random primary keys slow a MySQL insert workload even before the
   index exceeds the buffer pool? What changes after it exceeds it?
8. UUIDv7 vs Snowflake: name a system where each is the right answer, and
   the property that decides it.
9. Why is Redis `INCR` per id 40× slower than local generation and still
   sometimes the right choice? What did it buy you?
10. Your ids are 63-bit and time-prefixed. A product manager asks to
    shard by `id % 1024`. What goes wrong, and which bits should they
    shard on instead?

## 7. File map

| File | What it does |
|------|--------------|
| `docker-compose.yml` | MySQL 8.0.43 (:3319), Redis 7.4 (:6391), optional adminer (:8093, profile `ui`) |
| `Makefile` | every drill and gate; `make demo` is the whole story |
| `sql/schema.sql` | `worker_leases` (pre-seeded 0..15) and the `seq_autoinc` comparison table |
| `scripts/common.py` | env config, MySQL/Redis connections, jsonl journals |
| `scripts/bootstrap.py` | applies the schema, seeds the lease slots |
| `scripts/snowflake.py` | the bit layout, injectable clocks, naive + hardened generators; selftest with the broken-layout proof |
| `scripts/lease.py` | claim/heartbeat/release with owner tokens, the local validity window, lease event journal |
| `scripts/drill_clock.py` | one backwards jump vs naive/error/wait/hold, exact expected counts |
| `scripts/drill_exhaustion.py` | 5000 draws in one frozen ms; naive wraps, hardened spins |
| `scripts/drill_zombie.py` | SIGSTOP past TTL, id reclaimed; `ZOMBIE_NAIVE=1` guarantees the collision via a shared scripted clock |
| `scripts/verify.py` | the invariant gate; `VERIFY_INVERT=1` must catch the naive journals |
| `scripts/alternatives.py` | UUIDv7 (RFC 9562), AUTO_INCREMENT and Redis INCR makers |
| `scripts/bench.py` | five schemes × four processes; rank-displacement sortability metric |
| `scripts/bench_btree.py` | 200k-row insert into uuid4- vs uuid7- vs snowflake-keyed tables |
