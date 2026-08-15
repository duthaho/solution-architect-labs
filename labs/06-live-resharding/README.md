# Lab 06 — Live Resharding: Split One Big Table into N Shards Under Traffic

> Move a 500k-row `orders` table from one overloaded MySQL onto 2 shards keyed
> by `user_id`, with continuous reads and writes, **zero downtime**, a rollback
> that works **after** the cutover — and a verifier that proves no row was
> lost, duplicated, misplaced, or stale. Vitess does this for you in
> production; here you hand-roll every step small enough to understand it.

## 1. The problem

Lab 02 changed a table's *shape* under traffic. This lab changes its
*location* — the last resort when one primary simply cannot carry the write
load, the working set, or the blast radius anymore. Sharding is famously "the
thing you do too late", because the migration is scarier than the pain.

The naive options, and how they fail:

| Approach | Failure mode |
|---|---|
| `mysqldump` → import → repoint | Hours of write downtime; and the dump is stale the moment it starts. |
| Replicate to 2 new replicas, then cut over, then delete the other half from each | Gets you a *copy*, not a *shard*: both new nodes still carry 100% of the writes through replication until the very end; the delete of the unwanted half is itself a giant online migration. Workable (this is "split by replica"), but the cutover is still a hard flip with no rehearsal and no per-row verification. |
| Dual-write with 2PC/XA across mono + shard | Couples the monolith's availability to every shard: any node down ⇒ all writes fail. You wanted to *reduce* blast radius. See §3.3. |
| "Just use Vitess" | Often the right call! But Vitess's `MoveTables`/`Reshard` runs exactly the ladder below — and when its vdiff pages you at 3am, this lab is what tells you what to do. See §6. |

The real playbook is a **cutover ladder** — the same shape as gh-ost's
(lab 02), one level up the stack:

```
double-write → backfill → verify → shadow-read → flip reads+writes → (rollback window) → cleanup
```

Every rung is *reversible* and *rehearses* the next one. The flip itself is
one `rename()` syscall on a state file. That is what "zero downtime" actually
means: not that nothing changes, but that no single step is big enough to
fail loudly.

## 2. Architecture

```
 traffic.py ──► Router (a LIBRARY in common.py — not a proxy)
                  │ mode from router_state.json (atomic rename to flip)
                  │ modes: single → double-write → shadow-read → sharded
        ┌─────────┼──────────────┐
        ▼         ▼              ▼
   mysql-mono   mysql-shard0   mysql-shard1
    :3312        :3313          :3314
                shard = crc32(str(user_id)) % 2
```

| Mode | Writes | Reads |
|---|---|---|
| `single` | mono | mono |
| `double-write` | mono **first** (authoritative, this is the ack), then full-state upsert onto the owning shard | mono |
| `shadow-read` | as double-write | served from mono, **re-executed** on the shard, mismatches journaled |
| `sharded` | owning shard first (authoritative), then mirrored back onto mono | owning shard |

The consistency model, one sentence — this lab's version of lab 02's "binlog
always wins, backfill never fights":

> **Only the authoritative write can fail the request; every non-authoritative
> leg is a full-state upsert (late or missing rows self-heal on the next
> touch); the backfill only ever `INSERT IGNORE`s (it can never overwrite a
> fresher double-written row).**

Correctness is measured in three places, deliberately redundant:

- **At the app** (`traffic.py`): per-user invariant `COUNT(*) == MAX(seq) ==
  acked inserts`, asserted through the *live read path* every few ops. A lost
  row fails within seconds, mid-reshard — not in a post-mortem.
- **At rest** (`verify.py` checks 1–2): per-shard counts + content checksums
  vs mono's partitions, plus a misplacement scan.
- **On the read path** (check 4): the shadow-read diff journal must be empty.

## 3. Deep dives

### 3.1 The shard key, and the death of AUTO_INCREMENT

`user_id` is the shard key because the app's queries are user-scoped: one
user's orders land on one shard, so the hot path never crosses shards.
Choosing the key is 80% of the design:

- **Pick the key most queries carry.** Any query *without* `user_id` must now
  scatter-gather across every shard (or consult a secondary lookup table
  mapping e.g. `order_ref → user_id`). Sharding doesn't remove those queries;
  it makes their cost visible.
- **Hash, don't range.** `crc32(user_id) % 2` spreads sequential user ids
  evenly. Ranges (`user_id < 1M → shard0`) concentrate new-user growth — and
  new users are the active ones — onto the newest shard.
- **The verifier must be able to compute it in SQL.** `shard_for()` in Python
  is `zlib.crc32(str(user_id))`; the misplacement scan and partition filters
  use MySQL's `CRC32(user_id)` — identical because MySQL coerces to string.
  `bootstrap.py` cross-checks the two before anything runs. Never trust a
  hash function you haven't cross-checked from both sides.

And note what the schema does **not** have: an `AUTO_INCREMENT` id. A global
counter dies the moment there are two writers — shard0 and shard1 would both
mint id 500001. The PK here is `(user_id, seq)`: unique, orderable, and every
row's owner is computable from its own key. Real systems that must keep a
single id column switch to app-minted ids (Snowflake/ULID) or interleaved
increments (`auto_increment_increment=N, offset=i` — and regret it at N→N+1).

### 3.2 Why the ladder's order is load-bearing

The backfill refuses to run unless the router is already in `double-write`
(override with `--force` if you want to watch it go wrong — that's manual
drill 4). The safety argument is a three-legged stool; remove any leg and
there is a hole:

1. **Double-write first.** Once it's on, every row inserted *or updated* is
   delivered to its shard as a full-state upsert. The backfill's job shrinks
   to: rows that predate the flip.
2. **Backfill = `INSERT IGNORE`.** Consider the race: backfill reads a chunk
   from mono (row R at version v1) → app updates R (mono v2, shard upsert v2)
   → backfill inserts its stale v1. With a plain upsert the backfill would
   clobber v2 with v1 — a stale row that no count can see. With `INSERT
   IGNORE` the stale copy is dropped on the floor. Lab 02 closed the same
   race with `FOR SHARE` + binlog ordering; here the double-write leg *is*
   the fresher writer, so "never overwrite" is sufficient.
3. **Updates are upserts on the shard.** An UPDATE arriving for a row the
   backfill hasn't delivered yet would match zero rows and vanish. As an
   upsert carrying full state, it *creates* the row — and the backfill's
   later `INSERT IGNORE` bounces off it harmlessly.

This only holds because writes carry **full row state** and are applied
**per-key in order** (one router, sequential ops). With concurrent writers
per key you'd need per-key ordering (queues, or version-guarded upserts) —
that's the first thing to check in any real double-write design.

Why no deletes in this lab's traffic? Under an `INSERT IGNORE` backfill, a
delete leaves nothing behind to say "this absence is deliberate" — the
backfill would resurrect the row from a stale mono snapshot. Handling deletes
needs tombstones or a binlog stream (lab 02's machinery). The interview
question writes itself; see §7.

### 3.3 The partial-write policy (make it explicit, or an outage makes it for you)

In `double-write`, what happens when the shard write fails? Three choices:

| Policy | Consequence |
|---|---|
| Fail the request | Shard availability now gates the monolith — you *added* a point of failure before migrating anything. |
| 2PC across both | Same coupling, plus blocked transactions holding locks when the coordinator dies. |
| **Ack on authoritative, queue the failed leg** ✔ | The user's write is safe on the authoritative node. The missed leg is appended (full state) to `repair_queue.jsonl` and replayed by `backfill.py --repair` / `rollback.py` — idempotent, because it's the same upsert. |

Queue-and-repair is correct here *because* the non-authoritative copy is
allowed to be stale between repairs — the ladder never reads it
authoritatively until it has been verified. The one sharp edge: replaying a
queued state can clobber a *newer* live write to the same row. Production
systems guard the upsert with a version/timestamp column; this lab keeps the
queue small and drains it before every flip instead, and §6 lists the guard.

Manual drill 5 makes the policy visible: `docker stop lab06-shard0` during
double-write. Traffic stays green (mono is authoritative), the queue grows,
`make backfill` (which runs `--repair`) heals the shard after restart.

### 3.4 Shadow reads: rehearse the read path, not just the data

Checksums prove the *bytes* moved. They do not prove that the *read path* —
router logic, shard function, connection config, collation, timezone — will
return the same answers. `shadow-read` mode serves every read from mono, then
re-executes it on the owning shard and journals any mismatch to
`shadow_diffs.jsonl`. The gate for cutover is a boring soak: **N minutes, 0
diffs** (this is Vitess vdiff / GitHub's scientist pattern, hand-rolled).

Because the router writes both legs synchronously, a diff here is never
"replication lag" — it is a real bug. In an async production pipeline you'd
tolerate-and-recheck instead. Note `phase.py` *touches* the diff file when
entering the mode: "exists and is empty" (rehearsed, clean) must be
distinguishable from "never ran".

### 3.5 Cutover, and why rollback stays possible after it

`make cutover` is `phase.py sharded`: one atomic rename of
`router_state.json`. No locks, no pause — the running traffic generator picks
up the new mode within an op or two. The shards become authoritative and the
ladder's symmetry appears: **`sharded` mode keeps mirroring every write back
onto mono**, exactly as `double-write` mirrored mono onto the shards.

That mirror is not paranoia — it *is* the rollback feature. Mono never stops
being current, so `rollback.py` is a queue-drain plus one rename, not a
reverse migration:

1. drain `repair_queue` → mono (mirror legs that failed),
2. reconcile mono vs shards **while still sharded** — only now are both
   sides written for every op, so a live snapshot converges on retry. After
   the flip, mono runs *ahead* of the shards by design and the comparison
   stops meaning anything (the first version of this lab got that wrong and
   the rollback drill caught it),
3. flip to `single`,
4. final drain for legs that raced the flip.

The soak window costs a dual write per op. Ending it — "cleanup", when you
stop the mirror and truncate mono — is the last decision of the reshard, and
it is the moment rollback expires. Do it on purpose, on a date, after the
error budget says the shards have earned trust. Never let it expire by
forgetting.

### 3.6 Verification: counts lie, checksums don't, and verify the verifier

`verify.py`'s partition check computes, per user, `COUNT(*)` **and**
`SUM(CRC32(CONCAT_WS(...)))` over the value columns, comparing each shard
against mono filtered to that shard's partition. Two details that matter:

- **Counts alone are a lie.** A stale or corrupted row keeps the count
  intact. `drill-detect` proves the point: it flips one row's `amount` on a
  shard — same row count, different bytes — and requires the verifier to
  FAIL, then repairs from mono and requires it to pass. A verifier you have
  never watched fail is decorative.
- **Live traffic means no atomic snapshot — and the skew scales with the
  scan.** The first version of this check fingerprinted the whole partition
  and retried until two passes agreed. At 60k rows that worked; at 500k the
  aggregate scan takes ~1s per side, the two snapshots are always dozens of
  in-flight inserts apart, and the retry *never* converges — the full-scale
  demo failed its own gate. The fix is to shrink the comparison window, not
  the traffic: one `GROUP BY user_id` pass per side flags divergent users as
  *suspects*, then each suspect is re-read back-to-back (a millisecond
  window). In-flight skew clears on the first re-check; real divergence
  never does. Re-check-the-key-before-paging is exactly how production
  reconcilers (and Vitess's vdiff retry) behave. `updated_at` is excluded
  from the checksum because the two legs of one logical write are stamped at
  different times.

`journal.jsonl` (every acked write) is replayed as ground truth against
whichever side is currently authoritative, and any `check_fail` the app
recorded live is a verification failure even if the data later healed.

## 4. Runbook

```bash
make up install bootstrap seed    # 3× MySQL; schema everywhere; 500k rows on mono
make traffic-start                # live traffic through the router (mode: single)

make double-write                 # rung 1 — shards start receiving new writes
tail -f traffic.log               #   watch: repairs queued = 0, failures = 0
make backfill                     # rung 2 — chunked, throttled, resumable copy
make verify                       # rung 3 — the pre-cutover gate (retries under traffic)
make shadow-read                  # rung 4 — reads rehearsed on shards
sleep 60; make verify             #   gate: "shadow diff journal ... EMPTY"
make cutover                      # rung 5 — shards authoritative, mono mirrored
make traffic-stop                 # nonzero exit if the app ever saw a bad state
make verify                       # full: partitions, misplacement, journal, shadow
```

Expected shape of the end state:

```
CHECK 1: ... shard0 OK: 252417 rows, 993 users match mono partition (3 in-flight suspects cleared on re-check)
CHECK 2: ... 0 rows outside its partition
CHECK 3: ... replayed 5731 acked writes over 2000 users: 0 missing, 0 stale
CHECK 4: ... shadow diff journal exists and is EMPTY — read path proven
✅ VERIFIED: 0 lost, 0 duplicated, 0 misplaced, 0 stale
```

Or run the whole ladder in one shot: `make demo` (~3 minutes). Roll it back
afterwards: `make traffic-start drill-rollback`.

## 5. Failure drills

| # | Drill | What it proves |
|---|---|---|
| 1 | `make drill-crash-backfill` | `kill -9` mid-backfill; the rerun **resumes** from the chunk journal (state written only *after* each chunk commits — the replayed chunk is free because `INSERT IGNORE` is idempotent). |
| 2 | `make drill-detect` | Inject one corrupted row (same count, different bytes) → verifier MUST fail → repair from mono → clean. Never trust a verifier you haven't seen fail. |
| 3 | `make drill-rollback` | After cutover, under live traffic: drain, reconcile pre-flip, flip to mono. Traffic never stops; journal replays clean. |
| 4 | manual: `.venv/bin/python scripts/phase.py sharded` right after `double-write`, skipping backfill | Reads on the shards miss every pre-double-write row; `traffic.log` fills with `CHECK FAIL ... count < expected` within seconds. The ladder's ordering is load-bearing — and the app-level invariant is what catches an operator who skips a rung. Roll back with `phase.py single` (the pre-cutover rollback: trivial, mono was never demoted). |
| 5 | manual: `docker stop lab06-shard0` during `double-write` | Traffic stays green (authoritative-first); `repair_queue.jsonl` grows; `docker start lab06-shard0 && make backfill` heals it. The partial-write policy of §3.3, observed. |

## 6. Production checklist (when it's 500M rows and a pager)

- **Throttle the backfill** (`--chunk-sleep-ms`, start at 50) and watch the
  *authoritative* node's p99, not the backfill's rows/s. The copy is the
  lowest-priority writer in the system.
- **Guard repairs with a version.** Add `version BIGINT` (or trust a
  microsecond `updated_at`), and make every non-authoritative upsert
  `... ON DUPLICATE KEY UPDATE ... IF(new.version >= version, ...)`. That
  closes §3.3's clobber window and makes repair replay safe at any time.
- **Per-key write ordering.** Multiple app instances = concurrent writers per
  user. Either route each user's writes through one queue/worker, or rely on
  the version guard above. Full-state upserts without ordering are how shards
  silently diverge.
- **Each shard is a replica set**, not a single node — this lab's shard0 is
  production's shard0 primary + replicas + lab 04's failover drill.
- **Resharding 2 → 4** with `crc32 % N` moves *half of all rows*
  (`%2`→`%4` reassigns every odd crc). Production uses many virtual buckets
  mapped to shards by a **directory** (`bucket → shard`, itself in a store
  you can update transactionally), or consistent hashing. Buckets make the
  next reshard a directory edit + per-bucket copy — i.e., this same ladder,
  per bucket.
- **Scatter-gather and secondary keys**: queries without the shard key need a
  fan-out layer with partial-failure semantics, or a lookup table
  (`order_ref → user_id`) that is itself dual-written and verified. Budget
  for it before the cutover, not after the first `WHERE order_ref = ?` 500s.
- **Cross-shard transactions: don't.** Restructure so invariants are
  per-shard-key, or use sagas with compensation. XA across shards resurrects
  every problem sharding was meant to kill.
- **Hot shards**: one whale user still lands on one shard. Mitigations, in
  order: cache their reads, split *their* traffic by a secondary dimension,
  or give whales dedicated shards via the directory.
- **What Vitess automates** (`MoveTables`: VReplication streams ≈ our
  double-write+backfill via binlog; vdiff ≈ verify.py; `SwitchTraffic`
  ≈ phase.py, with reverse replication ≈ our mirror-for-rollback) — **and
  what stays human**: choosing the shard key, the soak length, the query
  patterns that must not scatter, and when rollback expires. The ladder is
  the same; only the typing is outsourced.
- **Deletes exist in real tables.** Add tombstones, or drive the sync legs
  from the binlog (lab 03's CDC) instead of app-level double-writes — then
  the backfill/catch-up race is closed by log ordering, as in lab 02.

## 7. Interview questions

Answer these without notes before moving on:

1. Why must double-write be ON before the backfill starts? Walk the exact
   interleaving that loses an update if the backfill upserts instead of
   `INSERT IGNORE`s.
2. Your double-write acks after mono but the shard write fails. Enumerate the
   three policies and their failure modes. Why is 2PC the worst of the three
   here?
3. The verifier shows equal counts everywhere. Are you safe to cut over? What
   class of bug do counts miss, and what catches it?
4. Why does rollback-after-cutover require a decision made *before* cutover?
   What exactly expires when you stop the mirror?
5. A row for user 42 sits on the wrong shard with correct values. What reads
   break, and why is this worse than the row being missing?
6. How do you reshard 2 → 4 without moving half the data? (Buckets/directory
   vs consistent hashing — and why `% N` is the trap.)
7. Where did `AUTO_INCREMENT` go, and what would you do if the API contract
   required a single opaque `order_id`?
8. Your traffic has hard DELETEs. What breaks in this lab's design, and which
   two mechanisms fix it?

## 8. File map

```
docker-compose.yml      mono :3312, shard0 :3313, shard1 :3314 (plain MySQL 8)
sql/schema.sql          orders(user_id, seq, ...) PK (user_id, seq) — no auto-inc
scripts/common.py       the Router (modes, authoritative-first, repair queue), shard_for()
scripts/phase.py        atomic mode flips — this file IS the cutover
scripts/traffic.py      user-scoped ops + live per-user invariant checks + journal
scripts/backfill.py     per-shard chunked INSERT IGNORE copy, resumable, --repair
scripts/verify.py       partitions, misplacement, journal replay, shadow report
scripts/inject.py       break a shard row on purpose (+ --repair) — feeds drill-detect
scripts/rollback.py     drain → reconcile pre-flip → flip to single → drain
```

State files (`router_state.json`, `journal.jsonl`, `repair_queue.jsonl`,
`shadow_diffs.jsonl`, `backfill_state.json`) live in the lab root; `make
clean` removes them all along with the containers and volumes.
