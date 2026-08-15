# Lab 09 — Deploy with no data gap and no downtime (expand/contract)

> **Status: PLANNED — nothing implemented yet.**
> To continue in a fresh session, read this file top-to-bottom, then start at the
> first unchecked milestone in [Milestones](#milestones).

## The pitch

Every deploy where code and schema must change together is a distributed-systems
problem: for some window, **old code and new code run against the same database at the
same time**. This lab makes that window visible and hostile.

Scenario: `users.name` must become `first_name` + `last_name`. The naive path
(one deploy: ALTER + new code) is run first — under load, with a rolling deploy — and
the lab counts the 500s and the rows written in the old shape by lingering old pods.
Then the same change ships via **expand → migrate → contract**, blue-green, with a
client that asserts correctness on every request: **0 errors, 0 data gaps**.

## Architecture

```
client.py (continuous R/W traffic, asserts every response, journals results)
   │
   ▼
gateway (nginx) ──► app-v1 ×2   (reads/writes name)          MySQL :3311
             └────► app-v2 ×2   (reads/writes first/last)      users table
                    (rolling: containers replaced one at a time,
                     blue-green: full flip of upstream)
```

- **App** = tiny Python HTTP service (stdlib or flask, pinned), built as one image with
  `APP_VERSION` env choosing behavior — v1, v1.5 (expand-aware: dual-write, read either),
  v2 (new columns only). Multiple containers per version via compose scale.
- **nginx** as the traffic switch: rolling = swap upstreams one by one (compose
  stop/start per container); blue-green = rewrite upstream conf + reload.
- **client.py** is the truth-teller: constant create/read/update loop; every read
  validates the full name round-trips; journals errors + mismatches with timestamps so
  the README can show "error window: 0ms" vs the naive run's carnage.
- **Migration steps as scripts**, one per expand/contract phase, each safe to re-run.

## The ladder (README structure)

1. **Naive**: `ALTER` + deploy v2 in one shot, rolling. Old pods write `name`, new pods
   read `first_name` → NULLs, errors. Counted.
2. **Expand**: add nullable `first_name`/`last_name` (online, instant in 8.0 for ADD
   COLUMN). Deploy **v1.5**: writes both shapes, reads old shape. Zero old-pod breakage.
3. **Migrate**: chunked backfill of old rows (throttled, resumable — lab 02's pattern in
   miniature), then verifier proves both shapes agree on every row.
4. **Flip reads**: deploy v2 (blue-green): reads new columns, still dual-writes.
5. **Contract**: stop writing `name`, verify no readers (how? — README: query log /
   grep + soak time), drop the column. Run the "contract too early" drill first.
6. **Rollback story at every step**: which steps are reversible, which aren't
   (contract), and why that asymmetry dictates the ordering.

## File tree (target)

```
labs/09-expand-contract-deploy/
├── README.md / PLAN.md / docker-compose.yml / Makefile / requirements.txt
├── nginx/upstreams.conf.tmpl
├── app/app.py              # one file, APP_VERSION switches v1 / v1.5 / v2 behavior
├── sql/v1.sql              # users(id, name, ...)
└── scripts/
    ├── common.py
    ├── bootstrap.py / seed.py
    ├── client.py           # journaling correctness-asserting traffic
    ├── deploy.py           # --strategy rolling|bluegreen --version v1.5|v2 ...
    ├── expand.py           # add columns (online DDL)
    ├── backfill.py         # chunked, throttled, resumable
    ├── verify.py           # shape agreement + client journal error-window report
    └── contract.py         # drop old column (with --force to run the bad drill)
```

## Makefile targets

```
up / down / clean / install / bootstrap / seed
traffic-start / traffic-stop            # client.py, pid+log convention
drill-naive          # v1→v2 one-shot rolling under load → error/gap report
reset                # restore v1 state to run the good path after the naive one
expand / deploy-v15 / backfill / verify / deploy-v2 / contract
drill-early-contract # drop column while v1.5 still deployed → count the blast, restore
demo                 # naive (broken) vs full expand/contract (clean) comparison
```

## Failure drills

| # | Drill | What it proves |
|---|-------|----------------|
| 1 | `drill-naive` | Rolling deploy + coupled schema change = measurable error window and rows written in the wrong shape |
| 2 | `drill-early-contract` | Contract before all readers/writers are gone = instant 500s; contract is the irreversible step |
| 3 | kill `backfill.py` mid-run, rerun | Backfill is resumable and idempotent (chunk journal) |
| 4 | rollback from step 4 to v1.5, then to v1 | Which steps are reversible and what "reversible" costs (dual-write kept alive) |

## Interview questions (README)

Why expand/contract ordering is forced by rollback-ability; what "backward compatible"
means for schema vs API vs events; how long the compatibility window must last (longest
rollback horizon, not deploy duration); online DDL limits (from lab 02) vs additive
changes; how blue-green interacts with DB state (you can't blue-green a database).

## Milestones

- [ ] **M1 — Infra**: compose (MySQL 3311, nginx :8080, app image v1 ×2), bootstrap,
      seed, `client.py` + journal. Gate: traffic green against v1.
- [ ] **M2 — Deploy machinery**: `deploy.py` rolling + bluegreen against nginx.
      Gate: v1→v1 rolling redeploy under traffic, 0 client errors.
- [ ] **M3 — Naive drill**: v2 behavior in app, `drill-naive` + error/gap report in
      `verify.py`. Gate: nonzero, reproducible carnage numbers.
- [ ] **M4 — Expand path**: `expand.py`, v1.5 dual-write behavior, `backfill.py`
      (resumable), shape verifier. Gate: expand→v1.5→backfill under traffic, 0 errors.
- [ ] **M5 — Flip + contract**: deploy-v2 blue-green, `contract.py`,
      `drill-early-contract` + restore path. Gate: full ladder green end-to-end.
- [ ] **M6 — Polish**: `make demo` comparison, README deep-dive, root README → ✅,
      `make clean` pristine, trim PLAN.md.
