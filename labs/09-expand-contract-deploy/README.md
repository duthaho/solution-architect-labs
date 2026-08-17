# Lab 09 — Deploy With No Data Gap and No Downtime (Expand/Contract)

> `users.name` must become `first_name` + `last_name`. Ship it twice, under
> continuous correctness-asserting traffic: once the naive way (one deploy:
> ALTER + new code) — and count the 500s and the acked writes that silently
> die — then via **expand → migrate → contract** with rolling and blue-green
> deploys: **0 errors, 0 lost writes**, old column gone. The series capstone:
> every deploy where code and schema change together is a distributed-systems
> problem, because for some window old code and new code run against the same
> database at the same time.

## 1. The problem

A deploy is never atomic. Between the first new pod and the last old pod
dying, **two versions of your code run side by side against one schema** — a
rolling deploy makes the window minutes, blue-green makes it seconds, but
only the *traffic* flip is instant: the processes overlap regardless. And the
database outlives both: you can blue-green an app; **you cannot blue-green a
database.**

The naive PR — "rename `name` to `first_name`/`last_name`, migration + code
in one deploy, worked in staging" — fails on that window, in layers:

| Moment | Who breaks | What it looks like |
|---|---|---|
| migration runs, pods still old | every old pod | `Unknown column 'name'` → 500 on every request |
| copy done → column dropped | nobody *visibly* | `name` writes acked after the copy die with the column — **acked, then lost, no error to anyone** |
| new pods up, some rows old-shape | new pods | reads compose `NULL NULL` → data gaps served as truth |

This lab's naive drill measures all three (a run on this machine: **669
failures over an 18.3s error window, 26 acked writes lost** — with named
victims: `id=200140: acked 'Vint T0n782373', database now has 'Radia
T0n23364'`). Then the same change ships clean.

The real playbook is the same cutover ladder as labs 02 and 06, one level up
the stack — this time the two "datastores" being kept in sync are **two
columns in the same table**, and the router being flipped is **the deployed
code itself**:

```
expand (add) → dual-write → backfill → verify → flip reads → relax → stop old writes → soak → contract (drop)
```

## 2. Architecture

```
client.py (continuous R/W, asserts EVERY response, journals anomalies)
   │
   ▼
nginx :8080  ◄── upstreams.conf rewritten + `nginx -s reload`  (deploy.py)
   │
   ├──► app-blue-a / app-blue-b     ─┐   one image; /state/<pod>.version
   └──► app-green-a / app-green-b   ─┤   read ONCE at boot decides behavior
                                     ▼
                              MySQL :3311  users(id, name → first/last, ...)
```

The version ladder — the **only** thing that changes per deploy; the HTTP API
(`{"name": "First Last"}` in and out) never does, which is what "backward
compatible" means at the API layer:

| Version | Writes | Reads | Deployed via |
|---|---|---|---|
| v1 | `name` | `name` | (start state) |
| v1.5 | `name` **and** `first/last` | `name` | rolling |
| v2 | both | `first/last` | **blue-green** |
| v3 | `first/last` | `first/last` | rolling |

The deploy system (`deploy.py`) is two boring primitives — *write a file*,
*restart a container* — plus one traffic switch (rewrite `upstreams.conf`,
`nginx -s reload`, which is graceful: old workers finish in-flight requests).
Rolling drains each pod out of the LB before restarting it; blue-green boots
the idle color, health-checks it **while it receives no traffic**, then flips
the whole upstream set in one reload and keeps the old color running as the
instant-rollback path.

Correctness is measured in three independent places (deliberately redundant,
same philosophy as lab 06):

- **Live** (`client.py`): every read must equal the last acked write for that
  id; every anomaly journaled with a timestamp → the **error window** number.
- **At rest** (`verify.py shapes`): zero rows missing `first/last`; zero rows
  where `name` disagrees with its own split — proving app-split, Python-split
  and backfill-SQL-split all agree (bootstrap asserts this up front on the
  nasty cases: `"Mary Jane Watson"`, `"Prince"`, `"double  space"`).
- **Post-hoc** (`verify.py audit`): lab 04's RPO idea — replay every write the
  app ever ACKED (`client_state.json`) against the database *now*. Naive
  produces nonzero "acked but lost"; the ladder must produce exactly zero.

## 3. Deep dives

### 3.1 The ordering is forced by rollback-ability, not taste

Walk the ladder backwards and ask "can I undo this step?":

| Step | Reversible? | Why |
|---|---|---|
| expand (ADD nullable cols) | trivially | unused columns are inert |
| deploy v1.5 (dual-write) | trivially | v1 ignores the new columns |
| backfill | trivially | derived data, recomputable |
| flip reads (v2) | **yes — because dual-write continues** | v1.5 reads `name`, which v2 still writes |
| relax + v3 (stop old writes) | yes, redeploy v2 — then re-backfill `name` | the window where only re-*deploys* roll back |
| **contract (DROP)** | **no** | the data in `name` is gone |

Every reversible step moves *first*; the irreversible one moves *last*, after
three independent proofs (§3.4). That's the whole design rule: **the order of
the ladder is the order of increasing regret.** Rollback isn't free either —
"reversible" at step 4 costs keeping dual-writes alive; the moment v3 ships,
rolling back past it requires re-backfilling `name`. Which is why the
compatibility window must last as long as your **longest rollback horizon**
(how far back might you ever need to redeploy?), not as long as the deploy
takes. In production that's days-to-weeks between v3 and the DROP, not this
lab's 8 seconds.

### 3.2 The relax step — found the hard way

The first version of this lab went v2 → v3 directly, and the client counted a
12-second window of 500s: `(1364, "Field 'name' doesn't have a default
value")`. v3 stops *writing* `name` — but `name` was still `NOT NULL` with no
default, so every v3 INSERT died **while the column existed**.

The lesson generalizes: **constraints are readers too.** `NOT NULL`, CHECK,
triggers, foreign keys — anything that *demands* the old shape must be
loosened before the writers leave, exactly as anything that *supplies* the
new shape must exist before readers arrive. Mirror-image of expand.

The cost asymmetry is worth knowing cold: `MODIFY name ... NULL` is an online
INPLACE **table rebuild** (7s on 200k rows here; hours on 500M — that's a
gh-ost job, lab 02), while `ALTER COLUMN name SET DEFAULT ''` is INSTANT
metadata-only. NULL-means-absent is cleaner semantics; a sentinel default is
O(1). Production picks per table size.

### 3.3 The mixed-window race the verifier caught

First full run of the good ladder, `verify.py shapes` failed with:

```
id=200206  name='Vint T1n64654'  first_name='Ada'  last_name='T1n430166'
```

During the v1 → v1.5 roll, on one row, this interleaving happened: a v1.5 pod
dual-wrote (name + first/last consistent), then a **still-alive v1 pod**
updated the same row writing `name` only. Result: `first/last` populated but
**stale** — and a backfill guarded by `first_name IS NULL` skips that row
*forever*. TTL would eventually have served a wrong name from the new
columns after the read flip.

The fix is lab 06's consistency rule wearing new clothes: until reads flip,
**`name` is the authoritative shape**, so the backfill's predicate is not
"missing" but "missing OR disagreeing", and its action is "recompute from the
row's current `name`" — one UPDATE per chunk, row-locked, convergent:

```sql
UPDATE users SET first_name = <split>, last_name = <split>
WHERE id > ? AND id <= ?
  AND name IS NOT NULL
  AND (first_name IS NULL OR first_name <> <split> OR last_name <> <split>)
```

A fresh v1.5 dual-write is consistent by construction → predicate false →
untouched. A mixed-window casualty → predicate true → repaired from the
authoritative column. Idempotent, so the crash-resume drill replaying a chunk
is a no-op. One row in ~5k ops hit this on a laptop; at production rates it's
guaranteed, and an `IS NULL` backfill ships the bug to customers.

### 3.4 Contract runs behind three proofs, because schemas can't see readers

Nothing in the schema tells you who still *reads* a column. So `contract.py`
demands three independent lines of evidence before the DROP:

1. **Intent**: every pod in the LB live-reports v3 via `/healthz` — asked,
   not assumed from the state file.
2. **At rest**: zero rows missing `first/last`.
3. **On the wire**: reset `performance_schema.events_statements_summary_by_digest`,
   soak, then scan every digest for `` `name` `` — exact backtick match, so
   `first_name` can't false-positive. Any statement touching the column
   fails the gate with the offending SQL printed.

The digest soak is the production trick (there: pt-query-digest / query-log
pipelines, soaked for **days** — cron jobs, monthly reports and that one
analyst's notebook all read columns at embarrassing frequencies). The lab
soaks 8 seconds; the mechanism is identical.

And the early-contract drill quantifies what the checks are worth: DROP while
v1.5 still serves → total outage (998 failures in ~13s here) — **but zero
acked writes lost**, because dual-writing meant every write also landed in
`first/last`, so `--restore` rebuilds `name` completely. Contract-too-early
at v1.5 is an *outage*; the naive path's drop is a *bereavement*. Dual-write
is what buys the difference.

### 3.5 What blue-green actually buys — and what it can't

Blue-green made the v2 read-flip window ~0: the green pair booted, warmed and
health-checked **with zero traffic**, then took 100% of it in one graceful
nginx reload, with blue kept running as an instant rollback path (the
rollback drill flips v2 → v1.5 → v2 under traffic, zero errors).

What it cannot do is version the database. Both colors point at the same
MySQL; there is no "old data" to flip back to. So blue-green narrows the
*code* overlap window but does nothing about the *schema* compatibility
window — the ladder underneath is still mandatory. "Backward compatible"
also means different things per layer, and interviews love this: for the
**API** it's the client contract (never changed here); for the **schema**
it's "old code can still write, new code can still read" (the whole ladder);
for **events/CDC** (lab 03) it's worse — consumers replay *history*, so an
old-shape event must stay decodable until the last consumer catches up, not
until the deploy finishes.

### 3.6 Online DDL cheat sheet (why each ALTER in this lab is safe)

| ALTER | Algorithm | Cost under traffic |
|---|---|---|
| ADD COLUMN (nullable, no default expr) | INSTANT (8.0.12+) | metadata only — 60ms on 200k rows here, same on 500M |
| MODIFY col NULL | INPLACE, rebuild | O(rows), online but IO-hungry; 7.2s / 200k |
| ALTER COLUMN SET DEFAULT | INSTANT | metadata only |
| DROP COLUMN | INSTANT (8.0.29+) | metadata only; earlier 8.0: INPLACE rebuild |
| one big `UPDATE ... SET` (the naive copy) | — | table-scan of row locks; blocks hot rows for seconds — this is why backfills are chunked (lab 02) |

Every ALTER in the good ladder is INSTANT except relax — and §3.2 shows how
to make even that one O(1) if you accept a sentinel default.

## 4. Runbook

```bash
make up install bootstrap seed     # infra + 200k users, blue pair on v1
make traffic-start                 # correctness-asserting traffic, journaled

# ACT 1 — watch the naive path burn (then look at the numbers)
make drill-naive                   # ALTER+copy+DROP, then rolling v1→v3
make reset                         # pristine v1 again

# ACT 2 — the ladder, every step under traffic
make traffic-start
make expand                        # ADD first/last  (INSTANT, nullable)
make deploy-v15                    # rolling: dual-write, read old
make backfill                      # chunked, throttled, resumable, convergent
make verify                        # journal + shapes + acked-write audit
make deploy-v2                     # BLUE-GREEN: flip reads in one reload
make relax                         # name nullable — v3 can't ship before this
make deploy-v3                     # rolling: stop writing name
make contract                      # 3 proofs, digest soak, then DROP
make traffic-stop                  # exits nonzero if the client saw ANY failure
make verify

make demo                          # all of the above, back to back
make status                        # who is deployed / who has traffic, live
```

Expected shape of the two endings:

```
# naive:
TRAFFIC SUMMARY: 2563 ops, 669 FAILURES, http=603 ... mismatch=34 null=32
ERROR WINDOW: 18307ms
acked-write audit: 396 acked writes, 26 LOST or mangled

# ladder:
TRAFFIC SUMMARY: 91427 ops, 0 FAILURES, ...
ERROR WINDOW: none — zero anomalies
acked-write audit: 18236 acked writes, 0 LOST or mangled
```

## 5. Failure drills

| Drill | What it proves |
|---|---|
| `make drill-naive` | Coupled schema+code deploy = measurable error window **and** silently lost acked writes. Reproducible numbers, not vibes. |
| `make drill-early-contract` | Contract before readers are gone = counted outage; but dual-write ⇒ `--restore` loses **zero** acked writes. Contract is the only irreversible rung. |
| `make drill-crash-backfill` | SIGKILL the backfill mid-run; rerun prints `RESUMING`, replayed chunk is a no-op, shapes verify clean. |
| `make drill-rollback` | v2 → v1.5 → v2 blue-green flips under traffic, zero client failures — reversibility is a property you *rehearse*, not assume. |

## 6. Production checklist (when it's 500M rows and a pager)

- **The compatibility window is a policy, not an accident.** Write down how
  long v(n-1) must stay deployable; the old column lives at least that long
  after v3. Teams die on "we dropped it Friday, the rollback was Monday".
- **Backfill throttling is adaptive there**: watch replica lag and p99, not a
  fixed `--chunk-sleep-ms`. Chunks keyed by PK ranges exactly as here (lab 02).
- **The relax step on a big table is itself a gh-ost migration** (INPLACE
  rebuild). Either schedule it like one, or take the `SET DEFAULT ''` route.
- **Find readers before contract with real telemetry**: digest tables reset
  and soaked for days, query-log pipelines, and grep the codebase — ORMs with
  `SELECT *` make every table's every column a read dependency. Fix the ORM
  mapping *first* (a `SELECT *` v2 would have broken at the DROP even though
  "v2 doesn't use name").
- **Dual-write in one statement** (as v1.5 does) or in one transaction —
  app-level "write A then write B" without atomicity recreates lab 07's
  stale-set race between the two columns.
- **Gate deploys on the migration state, not vice versa**: v1.5 must refuse
  to boot if `first_name` doesn't exist (fail fast at startup beats 500s at
  runtime). The lab's app skips this check; production shouldn't.
- **Feature flags don't replace the ladder** — a flag flips read behavior
  faster than a deploy, but the write-both / backfill / verify sequence is
  identical; flags just replace `deploy.py`.
- **Blue-green the stateless tier only.** Sessions, in-flight jobs, consumer
  offsets: anything stateful in the pods makes "just flip back" a lie.
- **Verify like the lab does**: an acked-write audit needs the *client's*
  journal, which production doesn't have — the substitutes are CDC-based
  reconciliation (lab 03) and shape checks (`shapes` here) run continuously,
  not once.

## 7. Interview questions

Answer without notes; every answer is somewhere above.

1. Why must expand come before the code that reads the new columns, and
   contract after the last code that touches the old one? What single
   property forces the *entire* ordering? (§3.1)
2. Your rolling deploy takes 4 minutes. How long must old and new schema
   shapes coexist? Why is "4 minutes" catastrophically wrong? (§3.1)
3. What does "backward compatible" mean for an API, a schema, and an event
   stream — and why is the event stream the hardest? (§3.5)
4. v3 stopped writing a `NOT NULL` column and every INSERT died. What are
   your two fixes, and what does each cost on a 500M-row table? (§3.2)
5. Why is an `IS NULL`-guarded backfill wrong under a rolling deploy? Write
   the correct predicate. (§3.3)
6. How do you prove nobody reads a column anymore, given the schema can't
   tell you? Name three independent signals. (§3.4)
7. Why can't blue-green roll back a schema change? What does the old color
   actually give you? (§3.5)
8. After the naive drop, 26 writes were acked and lost with zero errors
   logged anywhere server-side. Where's the only place that loss was even
   *observable*, and what's the production analogue? (§2, §6)

## 8. File map

```
docker-compose.yml     mysql :3311, nginx gateway, 4 app pods (2 per color)
nginx/nginx.conf       static config; includes the generated upstream set
app/app.py             THE app: one file, v1/v1.5/v2/v3 switched at boot
sql/v1.sql             users(id, name, email, updated_at)
scripts/common.py      config, split rule (Python + SQL twins), deploy state,
                       upstream writer, pod probes
scripts/bootstrap.py   schema, split-rule agreement proof, initial blue/v1 state
scripts/seed.py        200k users, old shape
scripts/client.py      the truth-teller: asserting traffic + anomaly journal +
                       acked-writes snapshot (no retries, ever)
scripts/deploy.py      rolling & blue-green over write-file/restart/reload
scripts/naive.py       the coupled migration (add + copy + DROP), for the drill
scripts/expand.py      INSTANT ADD COLUMN
scripts/backfill.py    chunked, throttled, resumable, CONVERGENT (§3.3)
scripts/contract.py    --relax / preconditions+soak+DROP / --force / --restore
scripts/verify.py      report (error window) / shapes / audit (acked writes)
scripts/reset.py       back to pristine v1 between acts
```
