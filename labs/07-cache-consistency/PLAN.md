# Lab 07 — Cache consistency: reproduce the stale-cache race, then kill it

> **Status: BUILT ✅** — all milestones done, `make demo` and `make drill-cdc`
> verified end-to-end from a pristine `make clean`. The full design, deep dives,
> runbook and drills now live in [README.md](README.md); this file is the
> build log.

## What got built

- Deterministic stale-set race via rendezvous hooks (`cache_client.py` +
  `race.py`): delete-on-write serves stale 10/10, the identical interleaving
  against versioned keys is fresh 10/10.
- Journaling soak + auditor: measured staleness (join of committed vs served),
  ttl ≈ TTL, delete/versioned ≈ 0ms, percentiles + timeline.
- Thundering herd (`herd.py`): 200-reader barrier stampede, naive 200x vs
  singleflight 1x vs stale-while-revalidate 1x with flat p99.
- CDC invalidation (`--profile cdc`, Debezium → Kafka → `invalidator.py`):
  staleness = pipeline lag, proved by killing the invalidator for 20s and
  watching the auditor's timeline climb and heal.
- `make demo` money table assembled from measured summaries only.

## Milestones

- [x] **M1 — Infra**: compose (mysql+redis, cdc profile stubbed), bootstrap, seed,
      `cache_client.py` with `ttl` + `delete` strategies, background traffic + auditor.
      Gate: soak runs, auditor prints staleness for `ttl`. *(max ≈ TTL: 4878ms @ TTL=5s)*
- [x] **M2 — Deterministic race**: hook system + `race.py`. Gate: `drill-stale-set`
      shows the stale read with a step-by-step interleaving printout, 10/10 runs.
- [x] **M3 — Versioned fix**: strategy + same-interleaving drill. Gate:
      `drill-versioned` clean 10/10; orphan-version cleanup explained in output.
- [x] **M4 — Herd**: `herd.py`, three protection modes, QPS table. Gate: `drill-herd`
      reproduces the spike (200x) and the single-flight fix (1x).
- [x] **M5 — CDC invalidation**: cdc profile (kafka+debezium), invalidator, lag-as-
      staleness measurement, kill-invalidator drill. Gate: `drill-cdc` bounded-staleness
      report incl. the outage bump *(healthy ≈ 0.5s, outage peak 23.8s, healed)*.
- [x] **M6 — Polish**: `make demo` money table, README deep-dive, root README → ✅,
      clean pristine, trim PLAN.md.
