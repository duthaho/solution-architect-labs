# Lab 05 — Distributed Locks Are a Lie: Fencing Tokens

Reproduce, **deterministically**, the corruption scenario from Martin Kleppmann's
["How to do distributed locking"](https://martin.kleppmann.com/2016/02/08/how-to-do-distributed-locking.html):
a worker holds a Redis lock, pauses past the TTL (here: `SIGSTOP`, in production:
a GC pause), another worker legitimately acquires the "same" lock, and the first
worker wakes up and writes anyway — *while still believing it is the sole owner*.

Then fix it the only way that actually works: **fencing tokens** — a monotonically
increasing number issued with the lock and **enforced by the storage layer**, not
by the lock holder. The lock can lie about who owns it. Storage that rejects
stale tokens cannot be fooled.

Smallest infra of the series (one Redis, one 100-line HTTP service, two worker
containers), biggest "aha".

```bash
make demo               # corrupt vs fenced, side-by-side verdict (~1 min)
```

---

## 1. The problem

You have a job that must not run twice concurrently: an invoice generator, a
compaction task, a "sync this account to the CRM" worker. Two instances running
at once corrupt data. So you reach for the standard recipe:

```
SET my-lock <me> NX PX 30000     -- acquire, auto-expire after 30s
... do the work ...
DEL my-lock                       -- release (compare-owner-then-delete)
```

The TTL is not optional. Without it, a crashed holder leaves the lock stuck
forever and the system deadlocks. With it, you have just signed a contract you
cannot keep: **"I will finish (or notice I'm out of time) before the TTL fires."**

No client can honor that contract, because no client controls its own clock time:

- stop-the-world GC pauses (the famous multi-minute full-GC war stories)
- VM live-migration and hypervisor CPU steal
- page faults against a swapping host, or an I/O stall on a sync write
- a network partition between acquiring the lock and using it
- the OS scheduler simply not running you (this lab's `SIGSTOP` is exactly that)

From the lock service's point of view all of these are identical: the holder
went silent, the TTL fired, the lock is free. From the holder's point of view
**nothing happened at all** — it wakes up mid-critical-section with no idea time
has passed, and keeps writing.

## 2. Why the naive approach fails

The failure sequence, which `make drill-corrupt` reproduces on demand:

```
worker A: acquire lock (ttl 2s) ── writes ── ██ PAUSED ██████████ ── writes ── !!
lock:                            ttl expires ┘
worker B:                                    acquire lock ── writes ────────────
                                             (correctly! the lock WAS free)
```

Note who misbehaved: **nobody**. A acquired correctly and wrote within what it
believed was its lease. Redis expired the key exactly as configured. B acquired
a genuinely free lock. Every component followed its spec, and the ledger still
ends up interleaved — because the overall system property ("at most one writer")
was never actually guaranteed by any component. It was an assumption living in
the gap between the lock's TTL and the client's ability to notice it.

Two reflexive "fixes" that don't work:

- **Longer TTL** — you're trading a longer outage after a real crash for a rarer
  (but still possible) corruption. Pauses have no upper bound; pick any TTL and
  a pause can exceed it. You've moved the cliff, not removed it.
- **Re-check the lock before every write** — `make drill-check-race` breaks this
  one deterministically. The check and the write are two operations; the pause
  can land *between* them. That's a time-of-check-to-time-of-use (TOCTOU) race,
  and it cannot be closed from the client side, because the client can be
  suspended at any instruction.

## 3. Concepts

### 3.1 Efficiency locks vs correctness locks

Kleppmann's most useful distinction. Ask: *what happens if two holders overlap?*

- **Efficiency lock** — you just do duplicate work: two workers both resize the
  same image, two cron runs both send a metrics ping. Annoying, costs money,
  corrupts nothing. A single Redis `SET NX PX` is *fine* here; fencing is
  over-engineering.
- **Correctness lock** — overlap corrupts state or violates an invariant:
  double-charging, interleaved file writes, split-brain writes to a database.
  Here a TTL lock **alone is never sufficient**, no matter how the lock service
  is built — the enforcement must move into the resource being protected.

Most production incidents in this area come from an efficiency-grade lock
quietly guarding a correctness-grade resource, working fine for two years
(`make drill-happy` shows why — it *does* work when nobody pauses), and then
one long GC.

### 3.2 Fencing tokens

A fencing token is a number issued together with the lock, with three required
properties:

1. **Monotonically increasing per resource** — every successful acquisition
   gets a strictly larger token than every previous one.
2. **Issued atomically with acquisition** — token generation and lock grant are
   one operation. If they were two round-trips, a client could acquire, pause
   *before* fetching its token, and end up holding a *higher* token than a later
   owner — monotonicity dies before you even start. In this lab it's one Lua
   script (`SET NX PX` + `INCR` server-side); in ZooKeeper/etcd the token is a
   by-product of the write that creates the lock, so it's atomic for free.
3. **Enforced by the storage layer** — the resource itself remembers the highest
   token it has ever seen and **rejects anything lower**. Equal is fine (one
   holder writes many times under one token); lower means "you are the past".

The deep insight: the client's *belief* about lock ownership stops mattering.
Worker A in `drill-fenced` still believes it holds the lock — read its log — but
its writes carry token 1, storage has seen token 2, and a single integer
comparison makes the zombie harmless. **Fencing doesn't fix the stale client;
it makes stale clients unable to do damage.**

### 3.3 Where tokens come from in real systems

| System | The token | Why it's monotonic |
|---|---|---|
| This lab | `INCR lock:fence` inside the acquire Lua script | Redis is single-threaded; INCR is atomic with the SET |
| ZooKeeper | `zxid` (global tx id) or the lock znode's `cversion` | Every ZK write gets a strictly increasing zxid from the leader |
| etcd | the key's `ModRevision` / lease grant revision | Raft log index; every write bumps the store revision |
| Google Chubby | "sequencer" — an opaque byte-string the holder passes to servers, who validate it with Chubby | Chubby invented this; the paper describes exactly the check-at-the-resource pattern |
| Postgres-as-lock | a `SERIAL`/sequence value taken in the same transaction that records the lease | The database serializes the transactions |

### 3.4 What if storage can't compare tokens?

The ledger in this lab cooperates: it stores `max_token_seen` and does one `<`
comparison. Lots of real storage can't run your code — but almost everything
offers *some* conditional primitive, and every one of them can carry fencing:

- **Compare-and-swap / conditional writes** — DynamoDB `ConditionExpression:
  token <= :mine`, S3 conditional requests (`If-Match` on ETag / `If-None-Match`),
  GCS object generation preconditions. Write the token into the object; make
  every write conditional on not regressing it.
- **Versioned objects / optimistic concurrency** — read version, write
  `WHERE version = :read`. The lock's token rides inside the row.
- **If truly nothing is conditional** (a plain filesystem, a legacy FTP drop):
  you cannot fence it, which means a TTL lock cannot make it safe, which means
  the design needs to change — funnel writes through a single process, or make
  writes idempotent/commutative so overlap stops being corruption (lab 08's
  territory).

The rule: **fencing state must be at least as consistent as the resource it
protects.** Manual drill 6 below shows what happens when it isn't.

## 4. Architecture

```
worker.py A ──┐ acquire(ttl) ─► ┌──────────────┐
              │  one Lua script │ redis :6380  │   SET lock <me> NX PX ttl
worker.py B ──┘  returns token  └──────────────┘   + INCR lock:fence  (atomic)
      │
      │ POST /append {owner, token, seq}            chaos.py:
      ▼                                             docker kill -s STOP worker-a
┌────────────────────────────────┐                  ...wait: TTL expired...
│ storage.py :8091               │                  ...wait: B mid-section...
│ append-only ledger             │                  docker kill -s CONT worker-a
│ --fencing off: accepts all     │
│ --fencing on : token < max ⇒ 409 │
└────────────────────────────────┘
```

Design decisions worth stealing:

- **Workers are containers, and worker.py is PID 1.** `docker kill -s STOP` then
  freezes exactly the process we mean to freeze — a perfect, scriptable
  stand-in for a stop-the-world pause. No `sleep()` in the worker pretending to
  be a pause: the worker genuinely does not know it was stopped, which is the
  whole point.
- **The drills are event-gated, not timed.** Chaos never sleeps and hopes; every
  step waits for an observed condition — "ledger has 2 entries from A", "lock
  key is gone", "B has written 2 entries" — before proceeding. That's why the
  corruption reproduces 10/10 instead of "usually".
- **Storage knows nothing about the lock.** It never talks to Redis, never
  checks who "holds" anything. It trusts only what's in the request. That
  asymmetry is the lesson: the component that can't be paused into a stale
  belief (because its state *is* the truth) is the one that must enforce.
- **Storage is single-threaded**, so `max_token` check-and-update is atomic by
  construction. In production this atomicity lives wherever your storage
  already serializes writes: a row lock, a CAS loop, a conditional PUT.

## 5. Runbook

```bash
make install          # host-side venv (drills drive docker + read the ledger)
make up               # redis :6380, storage :8091 (workers start per-drill)
make drill-happy      # 1. build false confidence
make drill-corrupt    # 2. destroy it
make drill-check-race # 3. destroy the popular "fix"
make drill-fenced     # 4. the real fix
make demo             # corrupt + fenced + comparison table
make clean            # pristine machine
```

### Drill 1 — `make drill-happy`: locks work fine (that's the trap)

A and B run back-to-back with no chaos. B's acquire politely retries until A
releases. Clean, contiguous ledger. This is the demo every "distributed lock in
5 lines of Redis" blog post ends with — and it's *true*, right up until the
first pause. Systems built on this pass code review, pass load tests, and run
in production for years before the ledger interleaves.

### Drill 2 — `make drill-corrupt`: the Kleppmann scenario

Watch the worker logs against the ledger timeline. The money shot:

```
[A] ACQUIRED lock, fencing token=1, ttl=2000ms — I believe I am the sole owner
[A] APPEND seq=2 ok    (then: frozen 3.5s by SIGSTOP — A has no idea)
[A] APPEND seq=3 ok    ← this lands INSIDE B's critical section
[A] RELEASE FAILED: lock is not mine anymore
```

A discovers the truth **at release time — after all six writes landed**. The
ledger shows A and B ping-ponging appends inside each other's "exclusive"
sections. Also note verify's output: the corruption is detected purely from
ledger order (a session's entries must be contiguous), no timing heuristics.

### Drill 3 — `make drill-check-race`: re-checking is not a fix

Worker A now runs `--mode recheck`: before *every* append it asks Redis "do I
still hold the lock?" and only writes on yes. Chaos waits for the log line
`CHECK_OK seq=2`, then freezes A in the gap between the check and the write
(the drill widens that gap to 2s so the freeze lands deterministically; a real
GC pause fits in a gap of any size, and it only has to win once).

```
[A] CHECK_OK seq=2: redis says I still hold the lock
      (frozen; TTL expires; B acquires, writes its ENTIRE section, releases)
[A] APPEND seq=2 ok          ← the already-approved write lands anyway
[A] CHECK_FAILED before seq=3 — aborting
```

The check catches the problem exactly one write too late. Every client-side
guard has this shape: *check, gap, act*. The pause goes in the gap.

### Drill 4 — `make drill-fenced`: same chaos, storage enforces

Identical chaos schedule to drill 2, storage started with `--fencing on`:

```
[A] APPEND seq=3 REJECTED 409 (token=1): token 1 < max_seen 2 — storage
    fenced me off. I believed I held the lock; I was wrong. Aborting.
```

The ledger stays contiguous, the rejected attempt is logged with its stale
token, and — read A's log carefully — **A still believed it held the lock when
it was rejected.** Nothing about A was fixed. It was made harmless. The 409 is
also the first *true* information A receives, long before its release fails.

### Drill 5 (manual) — partition instead of pause

```bash
# start drill-corrupt in one terminal; in another, at the moment A holds the lock:
docker network disconnect 05-fencing-tokens_default lab05-worker-a
# ...wait ~3s (TTL expires, B takes over)...
docker network connect 05-fencing-tokens_default lab05-worker-a
```

From the lock's perspective a partitioned client and a paused client are
indistinguishable: silence, then a zombie with stale beliefs. Every argument
above applies to partitions, slow disks, and clock trouble too — `SIGSTOP` is
just the most reproducible member of the family.

### Drill 6 (manual) — fencing state must be as consistent as the resource

Thought experiment you can build: run two storage replicas, each tracking its
own `max_token`, round-robin the workers across them. A's stale token-1 write
lands on replica 2, which has never seen token 2 → accepted. Fencing enforced
in a place that's *less* consistent than the data it guards protects nothing.
This is the same lesson as lab 04's fencing of a zombie primary: the fence must
live at (or serialize with) the single point the writes actually go through.

## 6. The Redlock debate

Redis' documented answer to "one Redis node is a SPOF for my lock" is
[Redlock](https://redis.io/docs/latest/develop/clients/patterns/distributed-locks/):
acquire the lock on a majority of N independent Redis nodes, using the TTL minus
elapsed time as the validity window.

- **Kleppmann's critique** (the article this lab reproduces): Redlock's safety
  depends on *timing assumptions* — bounded network delay, bounded process
  pauses, well-behaved clocks. A GC pause between acquiring the majority and
  using the lock defeats Redlock exactly the way it defeats a single node; a
  clock jump on one Redis node can expire a lock early. And crucially, **Redlock
  issues no fencing token**, so even a perfectly-behaving Redlock still leaves
  storage unprotected from zombies. His verdict: for efficiency locks it's more
  than you need; for correctness locks it's not enough.
- **Antirez's response** (["Is Redlock safe?"](http://antirez.com/news/101)):
  the clock assumptions are engineering-realistic (steady local clocks, no
  daemon-driven jumps), delays mostly affect *liveness* not safety, and a
  client can check its remaining validity window after acquisition. On fencing,
  his counter is that ordered tokens aren't always available and that unique
  tokens + conditional writes can achieve similar ends.
- **The part both sides agree on**, which is the part this lab teaches: if the
  resource can check *anything* (a token order, a version, a condition), the
  final word on safety belongs to the resource, not the lock. And if the
  resource can check something, a single Redis (or Postgres, or ZK) issuing
  monotonic tokens was already enough — the multi-node lock service is then
  solving an availability problem, not a correctness one.

## 7. Production checklist — laptop vs. pager

- [ ] **Classify every lock in your codebase**: efficiency or correctness? For
      efficiency locks, delete the ceremony — `SET NX PX` and move on. For
      correctness locks, answer "where is the fence enforced?" in the design doc.
- [ ] **The token must reach the resource.** Thread it through every write path
      the critical section touches — the job runner that takes a lock and then
      calls three services needs all three to enforce (or one chokepoint).
- [ ] **Alert on fencing rejections.** A 409-with-stale-token in production
      means a zombie fired live ammunition at your data and the fence caught
      it. That's a pager-worthy near-miss, not a debug log line.
- [ ] **Watch lease durations vs pause telemetry.** Export your GC max-pause
      and compare it to your lock TTLs. If p100 GC pause is 8s and your TTL is
      10s, you're two bad allocations from this lab's drill 2.
- [ ] **Zombie writes are retried writes.** After a 409 the worker must NOT
      retry the same payload with a fresh lock acquisition without re-reading
      state — the world changed while it was dead. Re-enter the critical
      section from the top.
- [ ] **Don't build fencing state in a cache.** `max_token_seen` in a Redis that
      can fail over and lose it re-opens the hole (drill 6). Keep it in (or
      transactional with) the protected data.
- [ ] **Prefer CP lock services for correctness locks** — ZooKeeper/etcd give
      you the token for free (`zxid`/revision) and don't lose lock state on
      failover. Then *still* enforce at storage.

## 8. Interview questions to answer without notes

1. Why is a TTL on a distributed lock both mandatory and unsafe? What exactly is
   the contract a TTL forces the client to sign?
2. Your colleague proposes "check the lock right before writing" — walk them
   through the TOCTOU race, then explain why *no* client-side check can close it.
3. Define the three properties a fencing token needs. Why must issuance be
   atomic with acquisition (what breaks if it's a second round-trip)?
4. Where must the token be checked, and why can't the lock service do it?
5. Your storage is S3 / DynamoDB / Postgres — how do you enforce fencing on
   each without a custom storage service?
6. Summarize the Kleppmann–antirez Redlock disagreement fairly. What do both
   sides agree on?
7. When is a plain Redis lock without fencing the *right* engineering call?
8. Lab 04 fenced a zombie MySQL primary; this lab fenced a zombie lock holder.
   What's the shared principle? ("The component with authoritative state must
   reject writers from the past" — split-brain is the same disease.)

## 9. File map

```
labs/05-fencing-tokens/
├── README.md               you are here
├── docker-compose.yml      redis :6380, storage :8091, worker template
├── Dockerfile              one python:3.11-slim image for worker + storage
├── Makefile                drills, demo, clean
├── requirements.txt        redis, requests (pinned)
└── scripts/
    ├── common.py           acquire+token Lua (atomic), release Lua, clients
    ├── worker.py           acquire → slow appends → release; logs its beliefs
    ├── storage.py          ~100-line ledger service; --fencing on|off
    ├── chaos.py            SIGSTOP/SIGCONT + event-gated waits (determinism)
    ├── drill.py            the four scenarios + comparison table
    └── verify.py           contiguity invariant + post-mortem timeline
```
