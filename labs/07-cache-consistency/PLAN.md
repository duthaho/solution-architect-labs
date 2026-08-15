# Lab 07 — Cache consistency: reproduce the stale-cache race, then kill it

> **Status: PLANNED — nothing implemented yet.**
> To continue in a fresh session, read this file top-to-bottom, then start at the
> first unchecked milestone in [Milestones](#milestones).

## The pitch

Everyone runs Redis in front of MySQL; almost nobody can state precisely when it serves
stale data, for how long, or why their invalidation strategy doesn't actually prevent
it. This lab makes cache races **deterministic** — no "run it 10k times and hope" —
by injecting controlled pauses at the exact interleaving points, then compares four
strategies on two measured axes: **staleness window** and **DB load under a herd**.

The two classic disasters, reproduced on demand:

1. **The stale-set race** (cache-aside's dirty secret): reader misses cache, reads DB
   (old value), *pauses*; writer updates DB, deletes cache key; reader wakes, sets the
   old value into cache → **stale forever** (until TTL). Delete-on-write did not save you.
2. **Thundering herd**: hot key expires, 200 concurrent readers all miss and hit the DB
   simultaneously. Measured as a DB-QPS spike graph in the terminal.

## Architecture

```
readers ×N ──► cache_client.py ──► Redis :6381 ──miss──► MySQL :3315
writers ×M          │                                        │
                    │ strategy: ttl | delete | versioned | cdc │
                    │ pause-injection hooks (the determinism)  │
                    ▼                                          │
              journal.jsonl ◄── staleness auditor ─────────────┘
                                          cdc mode: Debezium+Kafka (lab 03's compose
                                          pattern) → invalidator.py consumes binlog
                                          events → deletes/updates cache keys
```

- **cache_client.py** is the lab's core: one class per strategy, each with named
  **interleaving hooks** (`after_db_read`, `before_cache_set`, …). `race.py` drives two
  clients through an exact interleaving via those hooks (in-process threads +
  events — fully deterministic, reproducible in one run).
- **Strategies compared** (one column each in the final table):
  1. `ttl` — TTL-only, no invalidation. Staleness = up to TTL. The honest baseline.
  2. `delete` — cache-aside + delete-on-write. Vulnerable to the stale-set race (proved).
  3. `versioned` — writer bumps a per-key version in Redis (`INCR`); values stored under
     `key:v{n}`; readers fetch version then value. Old versions become unreachable →
     race neutralized; cost: extra round-trip + orphaned versions (TTL cleans them).
  4. `cdc` — Debezium tails MySQL binlog → invalidator deletes/re-fills keys. Bounded
     staleness = pipeline lag (measured); survives app-code bugs because invalidation
     is driven by what *actually committed*.
- **Herd protection** (orthogonal axis, demoed on the hot key): naive vs per-key
  mutex + promise (single flight) vs stale-while-revalidate. DB QPS printed per mode.
- **auditor.py**: writers journal every committed value with timestamps; readers journal
  every value served; auditor joins the two and reports staleness windows (max/percentiles)
  per strategy. Ground truth, not vibes.

## File tree (target)

```
labs/07-cache-consistency/
├── README.md / PLAN.md / docker-compose.yml / Makefile / requirements.txt
│                        # mysql :3315, redis :6381; kafka+debezium behind
│                        # `--profile cdc` so base drills stay light
├── sql/schema.sql       # products(id, price, version, updated_at)
└── scripts/
    ├── common.py
    ├── bootstrap.py / seed.py
    ├── cache_client.py      # strategies + interleaving hooks
    ├── race.py              # deterministic interleaving driver (the star)
    ├── herd.py              # hot-key expiry + N concurrent readers, QPS meter
    ├── traffic.py           # background journaling readers/writers (soak mode)
    ├── invalidator.py       # cdc consumer → cache invalidation
    ├── connector.py         # register Debezium connector (reuse lab 03 pattern)
    └── auditor.py           # staleness report from journals
```

## Makefile targets

```
up / down / clean / install / bootstrap / seed        (cdc profile: make up-cdc)
drill-stale-set      # deterministic stale-set race vs `delete` strategy → stale-forever shown
drill-versioned      # same interleaving vs `versioned` → race neutralized
drill-ttl            # TTL-only staleness window measured
drill-cdc            # up-cdc + soak traffic → bounded staleness = measured pipeline lag
drill-herd           # naive vs singleflight vs stale-while-revalidate, DB QPS table
soak                 # background traffic + auditor, all strategies, staleness percentiles
demo                 # the money table: strategy × (max staleness, herd DB QPS, cost/complexity)
```

## Failure drills

| # | Drill | What it proves |
|---|-------|----------------|
| 1 | `drill-stale-set` | Delete-on-write still serves stale-until-TTL under one precise interleaving — deterministically, every run |
| 2 | `drill-versioned` | The identical interleaving cannot produce a stale read with versioned keys |
| 3 | `drill-herd` | Hot-key expiry multiplies DB load by N; single flight caps it at 1 |
| 4 | `drill-cdc` + kill the invalidator mid-soak | CDC staleness is bounded by pipeline health — lag becomes staleness; restart → catch-up, auditor shows the bump |
| 5 | manual: set TTL=∞ with `delete` strategy | Why every cache entry still needs a TTL: the race turns "stale until TTL" into "stale forever" |

## Interview questions (README)

Walk through the stale-set race from memory; why delete-on-write beats update-on-write
(and still isn't enough); what bounds staleness in each strategy; where CDC invalidation
wins over app-driven (multi-writer, bugs, ORMs) and its costs; single flight vs
stale-while-revalidate trade-off; why "just use short TTLs" is a cost/staleness dial,
not a fix.

## Milestones

- [ ] **M1 — Infra**: compose (mysql+redis, cdc profile stubbed), bootstrap, seed,
      `cache_client.py` with `ttl` + `delete` strategies, background traffic + auditor.
      Gate: soak runs, auditor prints staleness for `ttl`.
- [ ] **M2 — Deterministic race**: hook system + `race.py`. Gate: `drill-stale-set`
      shows the stale read with a step-by-step interleaving printout, 10/10 runs.
- [ ] **M3 — Versioned fix**: strategy + same-interleaving drill. Gate:
      `drill-versioned` clean 10/10; orphan-version cleanup explained in output.
- [ ] **M4 — Herd**: `herd.py`, three protection modes, QPS table. Gate: `drill-herd`
      reproduces the spike and the single-flight fix.
- [ ] **M5 — CDC invalidation**: cdc profile (kafka+debezium), invalidator, lag-as-
      staleness measurement, kill-invalidator drill. Gate: `drill-cdc` bounded-staleness
      report incl. the outage bump.
- [ ] **M6 — Polish**: `make demo` money table, README deep-dive, root README → ✅,
      clean pristine, trim PLAN.md.
