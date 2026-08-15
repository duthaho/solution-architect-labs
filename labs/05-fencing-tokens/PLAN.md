# Lab 05 — Distributed locks are a lie: fencing tokens

> **Status: PLANNED — nothing implemented yet.**
> To continue in a fresh session, read this file top-to-bottom, then start at the
> first unchecked milestone in [Milestones](#milestones).

## The pitch

Reproduce, **deterministically**, the corruption scenario from Kleppmann's classic
"How to do distributed locking": a worker holds a Redis lock, pauses (GC / VM stall —
simulated with `SIGSTOP`), the lock expires, another worker acquires it, and the
paused worker wakes up and writes anyway — *while still believing it holds the lock*.

Then fix it the only way that works: **fencing tokens** — a monotonically increasing
number issued with the lock, **enforced by the storage layer**, not by the lock holder.
The lock can lie about who owns it; storage that rejects stale tokens cannot be fooled.

Smallest infra of the series, biggest "aha". Shares the split-brain theme with lab 04:
there fencing protects a database from a zombie primary; here it protects any resource
from a zombie lock-holder.

## Architecture

```
worker.py A ──┐  acquire(ttl=5s) ┌────────────┐
              ├─────────────────►│ Redis :6380 │  SET k v NX PX / INCR fence counter
worker.py B ──┘                  └────────────┘
      │ append (token?)                              chaos.py:
      ▼                                              docker kill -s STOP worker-a
┌───────────────────────────┐                        ... ttl expires ...
│ storage.py :8091           │                       docker kill -s CONT worker-a
│ append-only ledger (file)  │
│ --fencing off: accepts all │
│ --fencing on: rejects token < max_seen (409)      │
└───────────────────────────┘
```

- **Redis** = the lock service. Acquire: `SET lock owner NX PX ttl`; token: `INCR
  lock:fence` on successful acquire (token issuance must be atomic with acquire — Lua
  script; README explains why).
- **storage.py** = tiny HTTP service owning an append-only ledger. The protected
  invariant: entries must never interleave between owners within a critical section.
  With `--fencing on` it tracks `max_token_seen` and 409s anything lower.
- **workers run as containers** (not host processes) so `SIGSTOP`/`SIGCONT` via
  `docker kill -s` is clean and scriptable. Each worker: acquire → do N slow appends
  (deliberately slower than the TTL under chaos) → release.
- **verify.py** reads the ledger and checks the invariant; prints the exact interleaved
  writes when corruption happened, with tokens, as a timeline.

## The ladder (README structure)

1. **Happy path**: locks work fine when everyone is fast. (Why this breeds false trust.)
2. **The pause**: `drill-corrupt` — SIGSTOP the holder past TTL, B acquires, A resumes,
   both write. Ledger shows interleaving. *Checking "do I still hold the lock?" before
   writing does not fix it* — a drill variant proves the check-then-write race.
3. **Fencing**: same chaos, `--fencing on` — A's resumed writes get 409, ledger clean.
   Key point: A *still thought* it held the lock; correctness came from storage.
4. **What fencing requires**: monotonic tokens (Redis INCR here; zxid in ZK; revision in
   etcd) and a storage layer that can enforce ordering. What if storage can't? (CAS,
   conditional writes, versioned objects — S3/DynamoDB analogues in README.)
5. **Redlock discussion**: why multi-node Redis locking doesn't remove the need for
   fencing (timing assumptions), summarized fairly with both sides cited.

## File tree (target)

```
labs/05-fencing-tokens/
├── README.md / PLAN.md / docker-compose.yml / Makefile / requirements.txt
├── Dockerfile              # one image for worker.py + storage.py (python slim)
└── scripts/
    ├── common.py           # redis lock lua (acquire+token), http client helpers
    ├── worker.py           # acquire → slow appends → release; logs its own belief
    ├── storage.py          # ledger service, --fencing on|off
    ├── chaos.py            # SIGSTOP/SIGCONT schedules via docker kill -s
    ├── drill.py            # orchestrates a full scenario, collects logs
    └── verify.py           # invariant check + corruption timeline printout
```

## Makefile targets

```
up / down / clean / install
drill-happy          # no chaos, locks "work"
drill-corrupt        # SIGSTOP holder past TTL, fencing off → interleaved ledger
drill-check-race     # holder re-checks lock before each write → still corrupts (race)
drill-fenced         # same chaos, fencing on → 409s, clean ledger
demo                 # corrupt vs fenced, side-by-side timeline
```

## Failure drills

| # | Drill | What it proves |
|---|-------|----------------|
| 1 | `drill-corrupt` | TTL + pause = two owners; locks alone cannot protect storage |
| 2 | `drill-check-race` | "Check lock then write" is a TOCTOU race, not a fix |
| 3 | `drill-fenced` | Storage-enforced monotonic tokens survive the identical chaos |
| 4 | manual: `docker network disconnect` the holder instead of SIGSTOP | Partition looks identical to a pause from the lock's perspective |
| 5 | manual: two storage replicas with independent max_token | Fencing state must be as consistent as the resource it protects |

## Interview questions (README)

Why TTL locks are unavoidably unsafe without fencing; where the token check must live;
what property the token needs (monotonic per resource, atomically issued); ZK/etcd
equivalents; why Redlock is disputed; when a lock without fencing is still fine
(efficiency locks vs correctness locks — Kleppmann's distinction).

## Milestones

- [ ] **M1 — Infra**: compose (redis :6380, storage :8091, worker-a/b containers),
      Dockerfile, lock lua in common.py. Gate: `drill-happy` green.
- [ ] **M2 — Corruption**: chaos.py schedules, slow-append worker, verify.py timeline.
      Gate: `drill-corrupt` reproduces interleaving 10/10 runs.
- [ ] **M3 — The non-fix**: check-then-write variant. Gate: `drill-check-race` still
      corrupts (deterministically, with a forced pause between check and write).
- [ ] **M4 — Fencing**: token issuance + storage enforcement. Gate: `drill-fenced`
      clean 10/10; 409s visible in worker-a's log while it believes it holds the lock.
- [ ] **M5 — Polish**: `make demo` side-by-side, README deep-dive (incl. Redlock
      section, efficiency-vs-correctness), root README → ✅, clean pristine, trim PLAN.
