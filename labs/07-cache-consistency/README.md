# Lab 07 — Cache Consistency: The Stale-Set Race

Everyone runs Redis in front of MySQL. Almost nobody can state precisely **when
it serves stale data, for how long, and why their invalidation strategy doesn't
actually prevent it**.

This lab makes cache races **deterministic**. The classic "reader sets a stale
value right after the writer's delete" incident is not provoked by hammering the
system 10k times and hoping — it is *scheduled*, step by step, with rendezvous
hooks at the exact interleaving points, so it reproduces on run 1 and on run
10000. Then the same scheduled interleaving is replayed against a strategy that
survives it, and a journaling auditor measures — not asserts, measures — the
staleness of four strategies and the DB cost of a thundering herd.

```bash
make demo        # races + herd + measured staleness + the money table (~3 min)
make drill-cdc   # + kafka/debezium: staleness == pipeline lag, outage included (~3 min)
```

---

## 1. The problem

Cache-aside is four lines of pseudocode and everyone writes it the same way:

```
read(k):   v = cache.get(k); if miss: v = db.read(k); cache.set(k, v, TTL); return v
write(k,v): db.write(k, v); cache.delete(k)        # "delete-on-write invalidation"
```

It looks airtight: every write deletes the key, so the next read must refill
from fresh data. The bug is that **`db.read` and `cache.set` are two separate
steps with a gap between them**, and something can commit *in the gap*:

```
reader                         writer
------                         ------
GET p:42        -> miss
SELECT price    -> 10.00 (old)
   ║  (GC pause, scheduler, slow                 
   ║   response, retry, anything)
   ║                           UPDATE price = 99.00 (commit)
   ║                           DEL p:42        <- invalidation fires... into a vacuum
SETEX p:42 10.00               ...nothing left to invalidate
```

The delete happened. It just happened **before** the stale set. The cache now
serves 10.00 until the TTL expires, while the database says 99.00 — and if you
set a long TTL "because the delete keeps things fresh anyway", it serves 10.00
for hours. Delete-on-write turned your TTL from a freshness bound into your
**only** freshness bound, silently.

This is not exotic. Facebook documented exactly this class of race (and the
lease mechanism that fixes it) in the
[memcache paper](https://www.usenix.org/system/files/conference/nsdi13/nsdi13-final170_update.pdf);
every cache-aside deployment without leases/versioning has it today. The window
is a few microseconds wide on a happy day — which means it fires rarely,
silently, and only under load, the worst possible combination for debugging.

The second classic disaster needs no race at all: a **hot key expires**, and
every reader in flight misses at once. 200 concurrent readers = 200 identical
SELECTs hitting the DB in the same instant — a 200x load spike exactly when the
query behind the hot key was expensive enough to be worth caching. That's the
thundering herd, and `drill-herd` reproduces it with a barrier and counts the
actual SELECTs.

## 2. Architecture

```
readers ×N ──► cache_client.py ──► Redis :6381 ──miss──► MySQL :3315
writers ×M          │                                        │
                    │ strategy: ttl | delete | versioned | cdc │
                    │ hooks: named interleaving points         │
                    ▼                                          ▼
     journal_reads.jsonl                        journal_writes.jsonl
              └───────────► auditor.py ◄──────────────┘
                            (join: served price vs committed truth)

cdc profile:  MySQL binlog ─► Debezium ─► Kafka ─► invalidator.py ─► DEL p:{id}
                                                        └► journal_cdc_lag.jsonl
```

Two mechanisms carry the whole lab:

**Rendezvous hooks** (`cache_client.py` + `race.py`). Every step of the read
and write paths fires a named hook (`after_db_read`, `before_invalidate`, …).
Normally a no-op. Under `race.py`, a hook is a rendezvous: the reader thread
parks at `after_db_read` — exactly the gap in the diagram above — while the
driver runs the writer's complete write path, then releases the reader. The
race isn't *found*, it's *enforced*. That's what "deterministic" means here.

**The journals** (`traffic.py` + `auditor.py`). Writers journal every price
*after commit*; readers journal every price *as served*. The auditor joins
them: a read is stale iff it served a price that a newer committed write had
already superseded, and its staleness is *how long ago* the supersession
happened. Every number in the money table comes from this join. No strategy is
taken at its word.

## 3. Deep dives

### 3.1 Why delete-on-write, not update-on-write (and why it's still not enough)

The tempting "fix" is `db.write(k,v); cache.set(k,v)` — skip the refill
entirely. It's worse. Two writers W1(=10) then W2(=99) can commit in that order
in MySQL but have their `cache.set` calls land in the opposite order (W1 stalls
a beat), leaving 10 in the cache with **no TTL-refill path that ever corrects
it** — the next writer is the only thing that can fix the key. Delete-on-write
at least converges: deletes are idempotent and order-insensitive, and the worst
case of two racing deletes is an extra miss.

So: delete beats update. And `drill-stale-set` proves delete still loses to a
*reader* — because the reader's stale `SETEX` is an *update* in disguise. Any
scheme where a non-authoritative party writes values into the cache after
reading the DB has this hole. The fixes below all work by making the stale set
either impossible to address (versioned) or externally corrected (cdc).

### 3.2 TTL is a dial, not a fix

`drill-ttl` runs TTL-only (writes never touch the cache) and the auditor
reports max staleness ≈ the TTL, p50 ≈ half of it — exactly the theory. Short
TTLs don't *fix* staleness, they *price* it: halving the TTL halves the
staleness bound and roughly doubles the miss rate (and herd exposure) on hot
keys. It's a cost/freshness dial with no correct setting, only trade-offs. But
note what TTL does have: an **unconditional** bound. No race, no bug, no dead
pipeline can extend it. That's why §3.5's checklist keeps a TTL on every key
even when smarter invalidation exists: TTL is the backstop that turns "stale
forever" into "stale for at most TTL".

That's also drill 5: run the stale-set race with `--ttl 86400` and the stale
value's lifetime is 24h. The race turns "stale until TTL" into "stale until
whenever" — the longer your TTL, the more the race costs, and delete-on-write
tempts you into long TTLs precisely because it "keeps things fresh".

### 3.3 Versioned keys: don't prevent the stale set — make it unaddressable

The versioned strategy changes nothing about *when* the stale set happens. It
changes *where it lands*:

- Values live at `p:{id}:v{n}`. A version pointer `v:{id}` lives beside them.
- **Readers** resolve the pointer, then the value; on a miss they fill the
  version they resolved *before* their DB read. Readers never advance the
  pointer (first-touch initialization is `SET NX` — it can create, never move).
- **Writers** commit to MySQL, then `INCR v:{id}`. That INCR *is* the
  invalidation: it doesn't destroy the old value, it makes it unreachable.

Replay the exact interleaving from §1: the parked reader wakes and faithfully
sets 10.00 — under `p:42:v1`. But the writer already INCRed the pointer to 2.
Every subsequent reader resolves v2, misses, refills fresh. The stale write
landed in a mailbox nobody will ever open again. `drill-versioned` runs the
*same schedule* as `drill-stale-set` and comes out clean 10/10.

Costs, honestly: one extra Redis round-trip per read (pointer + value — or a
pipeline/Lua to merge them), orphaned version keys (the value TTL garbage-
collects them), and the pointer itself must not be evicted independently of
the values (same Redis, no per-key eviction surprises — or accept a cold miss).
This is the same idea as memcache leases and as lab 05's fencing tokens: **a
monotonic number, enforced at the storage layer, beats any amount of client
carefulness.**

### 3.4 CDC invalidation: the binlog doesn't forget to call delete()

Every app-driven strategy shares a quiet assumption: *every code path that
writes the DB also runs the invalidation step*. The ORM bulk-update someone
adds next quarter, the migration script, the DBA hotfix at 2am, the second
service that "just does one UPDATE" — none of them call your `cache.delete()`.

The cdc strategy deletes cache keys from the **binlog**: Debezium tails MySQL,
the invalidator consumes change events and DELETEs `p:{id}` for whatever
*actually committed*, no matter who committed it. The app's write path does
nothing at all. Properties worth stating precisely:

- **Staleness = pipeline lag + one miss.** Measured in `drill-cdc`: a few
  hundred ms healthy (mostly Debezium/Kafka batching), and during a deliberate
  20s invalidator outage the auditor's timeline shows staleness climbing one
  second per second, then collapsing when the consumer restarts and burns the
  backlog. Lag is staleness; monitor consumer lag and you are monitoring
  cache freshness.
- **Delete, don't re-fill.** The consumer could write the event's `after` image
  into the cache — but then it races concurrent readers doing their own fills,
  and proving the event is newer than what a reader just SET needs versioning
  anyway. Deletes are idempotent and order-insensitive; at-least-once delivery
  becomes harmless. Keep the consumer boring.
- **It's still asynchronous.** CDC gives you bounded, observable staleness and
  invulnerability to app-code bugs. It does not give read-your-writes: a client
  that writes and immediately reads can still see the old value for a lag's
  worth of time. If you need read-your-writes, that's a session-level concern
  (write-through for your own session, or versioned reads), not something any
  async invalidator can provide.

Note `snapshot.mode=no_data` in the connector config: an invalidator has no use
for a snapshot — deleting keys for rows that changed before the cache existed
is a no-op, and a cold cache is already correct. Only the stream matters.

### 3.5 The herd: single flight vs stale-while-revalidate

Orthogonal to every strategy above: what happens when a hot key misses *for
everyone at once*. `drill-herd` releases 200 readers through a barrier onto one
absent key, with the backing query modeled at 80ms (a key is only worth caching
if the query behind it costs something — and that cost is exactly what makes
the herd dangerous):

| mode | DB reads | what the readers experience |
|---|---|---|
| naive | **200** | all pay ~query time; the DB pays 200x |
| singleflight | **1** | one refills; 199 wait on the lock, then read the cache |
| stale-while-revalidate | **1** | one refreshes; 199 get the *old* value instantly |

Single flight (`SET lock:{k} NX PX …`): the first miss takes a per-key Redis
lock and refills; everyone else polls the key. Amplification capped at 1, but
the losers **queue** — p99 latency ≈ refill time. The lock has a TTL and losers
fall back to the DB after a bounded wait, so a crashed winner degrades the
protection to "naive", never to "unavailable". (Getting even this small lock
right is lab 05's whole subject.)

Stale-while-revalidate: the value carries a logical expiry inside the payload
and physically outlives it. Past logical expiry the first reader refreshes
while everyone else is served the expired value *immediately* — flat latency,
1 DB read, and a window of **admitted, bounded** staleness. That's the theme of
this whole lab in one mechanism: you don't eliminate staleness, you choose its
bound and make it explicit.

### 3.6 Reading the money table

From a `make demo` on this machine (your numbers will vary, their *shape* won't):

```
strategy     stale reads  staleness p99  staleness MAX  invalidation cost
ttl               80.1%        14191ms        14929ms   none
delete             1.9%            0ms            0ms   1 DEL per write
versioned          2.0%           10ms           10ms   +1 RTT per read
cdc (drill)       41.8%        22086ms        23796ms   pipeline (incl. 20s outage)
herd: naive=200 DB reads   singleflight=1   swr=1
```

Read it carefully — it's more honest than the folklore version:

- `delete` soaks **clean**. The stale-set window is microseconds wide; a 60s
  soak won't hit it. That's the trap: the strategy that loses *deterministically*
  to `drill-stale-set` looks perfect in any load test you'll ever run. Races
  don't show up in averages; that's why this lab schedules them instead.
- `delete` and `versioned` both show ~2% "stale" reads at ~0ms staleness:
  reads racing concurrent commits, i.e. the unavoidable inconsistency window of
  any async cache. The difference between 0ms-stale and 15s-stale is the entire
  subject.
- `cdc`'s ugly numbers *include the deliberate outage* — that's the point. Its
  healthy-state staleness is the timeline's quiet buckets (~0.5s); its worst
  case is exactly the outage length. Pipeline health IS the freshness contract.

## 4. Runbook

```bash
make up install bootstrap seed   # mysql :3315, redis :6381, schema, 1000 products

make drill-stale-set   # the race, scheduled: delete-on-write serves stale, 10/10
make drill-versioned   # identical schedule vs versioned keys: fresh, 10/10
make drill-ttl         # ttl-only soak: auditor measures staleness ≈ TTL
make drill-herd        # 200-reader stampede: naive vs singleflight vs swr
make soak              # journaling soak of ttl/delete/versioned + audit
make report            # the money table, from measured summaries

make drill-cdc         # kafka+debezium profile; healthy lag, then a 20s
                       # invalidator outage: watch staleness climb and heal
make clean             # pristine machine
```

`make demo` runs the first six in order. Journals accumulate per run and are
reset at each soak start; `audit_summary.json` merges across runs so the table
keeps every strategy you've measured so far.

## 5. Failure drills

| # | Drill | What it proves |
|---|-------|----------------|
| 1 | `drill-stale-set` | Delete-on-write serves stale-until-TTL under one precise interleaving — 10/10 runs, scheduled, not sampled |
| 2 | `drill-versioned` | The identical interleaving cannot produce a stale read once the stale set is unaddressable |
| 3 | `drill-herd` | Hot-key expiry multiplies DB load by N; single flight caps it at 1; swr also caps it and keeps latency flat, for the price of admitted staleness |
| 4 | `drill-cdc` (kills the invalidator mid-soak) | CDC staleness is bounded by pipeline health: lag becomes staleness one second per second, restart + backlog burn heals it — the auditor timeline shows the whole arc |
| 5 | manual: `race.py --strategy delete --expect stale --ttl 86400` | Why every entry needs a TTL even with "working" invalidation: the race's cost scales with the TTL you chose |

## 6. Production checklist (when it's a real fleet and a pager)

- **Every key gets a TTL. No exceptions.** TTL is the only freshness bound that
  survives every race, bug, and dead pipeline in this lab. Invalidation
  narrows the bound; TTL guarantees one exists.
- **Add jitter to TTLs** (`ttl ± rand(10%)`). This lab expires one hot key on
  purpose; production expires a thousand keys cached at the same deploy
  timestamp, simultaneously. Same herd, no drill needed.
- **Pick your herd protection per key class.** Single flight for
  can't-be-stale keys; stale-while-revalidate for the long tail where p99
  latency matters more than a second of staleness.
- **If you use delete-on-write, know you own this race.** Mitigations in
  ascending order of rigor: short TTLs (shrink the blast radius), memcache-style
  leases (the miss returns a token, the set is rejected if the token was
  invalidated — Facebook's fix), versioned keys (this lab's), CDC-driven
  deletes (kills the app-bug class too).
- **Monitor consumer lag as a freshness SLO** if you run CDC invalidation.
  The `drill-cdc` timeline is your dashboard: lag *is* staleness. Alert on it
  like you alert on replication lag — because that's what it is.
- **Never re-fill the cache from the CDC event payload** without a version
  check; deletes are the order-insensitive, at-least-once-safe verb.
- **A restarted cache must start cold, not warm-and-wrong**: persistence off
  (this lab's Redis runs `--save ""`), or explicit flush on promote. Cold is
  slow; wrong is an incident.
- **Measure, don't assert.** The auditor pattern — journal commits, journal
  serves, join offline — ports directly to production as a sampled shadow
  audit. The strategies' marketing claims are not observability.

## 7. Interview questions

Answer without notes, out loud:

1. Walk through the stale-set race. Why doesn't delete-on-write prevent it?
   How wide is the window, and why does that make it *worse*, not better?
2. Why is delete-on-write still preferred over update-on-write? (Two writers,
   opposite arrival orders — who fixes the cache, and when?)
3. What bounds staleness in each of: TTL-only, delete-on-write, versioned
   keys, CDC invalidation? Which of those bounds are unconditional?
4. Where does CDC invalidation beat app-driven invalidation, and what new
   dependency does it introduce? What's your freshness SLO metric there?
5. Single flight vs stale-while-revalidate: what does each cost the p99, the
   DB, and the freshness contract? When is each wrong?
6. Why is "just use short TTLs" a dial and not a fix? What two costs rise as
   you shorten it?
7. Your cache restarted and was warm within a minute. Why might that be the
   worst possible news?

## 8. File map

```
docker-compose.yml       mysql :3315, redis :6381; kafka + debezium behind --profile cdc
sql/schema.sql           products(id, price, version, updated_at)
connectors/products-source.json   Debezium source, snapshot.mode=no_data
scripts/
  common.py              config, connections, O_APPEND journals
  bootstrap.py seed.py   schema + flush + journal reset; 1000 products
  cache_client.py        THE CORE: 4 strategies + named interleaving hooks,
                         singleflight + swr read paths, db_reads instrumentation
  race.py                deterministic interleaving driver (rendezvous hooks)
  herd.py                barrier stampede, 3 protection modes, DB-read counting
  traffic.py             journaling soak readers/writers (hot-key skew)
  auditor.py             journal join -> stale%, staleness percentiles, timeline
  connector.py           register Debezium connector, wait RUNNING
  invalidator.py         kafka consumer -> DEL p:{id}, lag journal
  report.py              the money table from measured summaries
```
