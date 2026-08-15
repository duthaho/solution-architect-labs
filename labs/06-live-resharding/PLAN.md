# Lab 06 — Live resharding: split one big table into N shards under traffic

> **Status: PLANNED — nothing implemented yet.**
> To continue in a fresh session, read this file top-to-bottom, then start at the
> first unchecked milestone in [Milestones](#milestones).

## The pitch

The sequel to lab 02. There you changed a table's *shape* under traffic; here you change
its *location*: one overloaded `orders` table → 2 shards keyed by `user_id`, with
continuous reads and writes, no downtime, and a verifier that proves no row was lost,
duplicated, or misplaced. This is the Vitess/planetscale move, hand-rolled small enough
to understand every step.

The cutover ladder (same shape as gh-ost's, one level up the stack):
**double-write → backfill → verify → shadow-read → flip reads → flip writes → cleanup.**

## Architecture

```
traffic.py ──► router (library in common.py, mode from router_state.json)
                 │  modes: single → double-write → shadow-read → sharded
       ┌─────────┼──────────────┐
       ▼         ▼              ▼
  mysql-mono   mysql-shard0   mysql-shard1
   :3312        :3313          :3314
              shard = crc32(user_id) % 2      (README: why not mod on id ranges,
                                               consistent hashing for N→N+1)
```

- **3× MySQL**: the monolith (source of truth initially) + 2 shards.
- **Router is a library, not a proxy**: `common.py` exposes `execute(user_id, sql)`;
  behavior driven by `router_state.json` (atomic rename to change mode — same pattern
  as lab 04's router). Modes:
  1. `single` — all traffic to mono.
  2. `double-write` — writes to mono **and** owning shard (mono still authoritative;
     README: write to authoritative first, tolerate shard lag, and why 2PC is not the
     answer here).
  3. `shadow-read` — reads served from mono, *also* executed on shard, diffs journaled.
  4. `sharded` — shards authoritative; mono frozen.
- **backfill.py**: per-shard chunked copy from mono (`WHERE crc32(user_id)%2 = shard`),
  throttled, resumable via chunk journal — lab 02's machinery, reused conceptually.
- **verify.py**: (a) counts + checksums per shard vs mono partition-filtered checksums;
  (b) misplacement scan — every row on the shard that owns it; (c) shadow-read diff
  report (read-path proof, not just data-at-rest proof).
- **traffic.py**: user-scoped reads/writes/updates with a per-user sequence check so
  lost or stale rows surface as journal errors — correctness measured at the app.

## File tree (target)

```
labs/06-live-resharding/
├── README.md / PLAN.md / docker-compose.yml / Makefile / requirements.txt
├── sql/schema.sql          # orders(id, user_id, seq, payload, ...)  on all nodes
└── scripts/
    ├── common.py           # router: execute(user_id, sql) per router_state.json
    ├── bootstrap.py / seed.py            # seed 500k rows on mono
    ├── traffic.py          # journaling, per-user seq assertions
    ├── phase.py            # phase.py <single|double-write|shadow-read|sharded> (atomic)
    ├── backfill.py         # per-shard chunked, throttled, resumable
    ├── verify.py           # checksums, misplacement scan, shadow diff report
    ├── inject.py           # corrupt/delete a shard row (for the verifier drill)
    └── rollback.py         # from sharded back to single (mono catch-up story)
```

## Makefile targets

```
up / down / clean / install / bootstrap / seed
traffic-start / traffic-stop
double-write / backfill / verify / shadow-read / cutover   # phase steps
rollback
drill-crash-backfill    # SIGKILL backfill mid-run → rerun resumes, verify clean
drill-detect            # inject.py corrupts a shard row → verify catches it pre-cutover
drill-rollback          # after cutover, roll back to mono → traffic still correct
demo                    # full ladder under traffic, end verify: 0 lost / 0 dup / 0 misplaced
```

## Failure drills

| # | Drill | What it proves |
|---|-------|----------------|
| 1 | `drill-crash-backfill` | Backfill is resumable + idempotent (chunk journal, INSERT…ON DUPLICATE) |
| 2 | `drill-detect` | The verifier actually detects corruption — never trust a verifier you haven't seen fail |
| 3 | `drill-rollback` | Rollback after cutover: what the mono missed and how double-write symmetry (keep writing mono during soak) buys the rollback window |
| 4 | manual: flip to `sharded` while backfill incomplete | Reads miss rows; why the ladder's ordering is load-bearing |
| 5 | manual: kill one shard during `double-write` | Partial write handling: fail the request vs queue-and-repair; the consistency choice made explicit |

## Interview questions (README)

Choosing a shard key (and what happens to queries without `user_id` — scatter-gather,
secondary lookup tables); why double-write ordering is authoritative-first; resharding
2→4 later (consistent hashing vs directory); what Vitess automates vs what stays a
human decision; cross-shard transactions (avoid, or saga) ; hot-shard mitigation.

## Milestones

- [ ] **M1 — Infra**: compose (3× MySQL 3312–3314), schema everywhere, bootstrap, seed,
      router `single` mode + traffic green against mono.
- [ ] **M2 — Double-write**: router mode, phase.py, partial-write policy implemented +
      logged. Gate: traffic under `double-write`, new rows appear on owning shards.
- [ ] **M3 — Backfill + verify**: chunked resumable backfill, checksum + misplacement
      verifier, `drill-crash-backfill`, `drill-detect`. Gate: verify clean; both drills.
- [ ] **M4 — Shadow-read + cutover**: shadow mode with diff journal, cutover flip,
      post-cutover traffic green. Gate: shadow diff = 0 over a soak, then cutover.
- [ ] **M5 — Rollback**: soak-window design (mono kept written), rollback.py,
      `drill-rollback`. Gate: full ladder + rollback both green under traffic.
- [ ] **M6 — Polish**: `make demo`, README deep-dive, root README → ✅, clean pristine,
      trim PLAN.md.
