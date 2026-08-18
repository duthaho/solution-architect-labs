# Lab 10 — Soft delete for many big tables

A microservice team decides: *"we never hard-delete. Every table gets a
structurally identical copy in a `deleted` schema, and DELETE means moving the
row over there."* It sounds clean — the live tables stay lean, deleted data is
recoverable, auditors are happy. Is it sane at 100M+ rows under live traffic?

This lab builds that design **and** its two real competitors side by side, on
identically seeded data, under continuous writes — and makes each one's
failure mode reproducible on your laptop:

| | Strategy | One-line summary |
|---|----------|------------------|
| A | `deleted_at` column | flag the row, filter every query, rows never leave |
| B | mirror `deleted` schema | the case-study design: move the row family out in one transaction |
| C | flag + background archiver | instant soft delete for the user, batched move to an archive later |

## 1. The problem

Deleting a row is the easiest SQL statement there is — which is exactly why
it's dangerous. At scale, every naive answer fails a different way:

- **Hard DELETE** is irreversible the moment the transaction commits. Support
  tickets ("I deleted my account by mistake"), auditors ("show me what was
  removed and when"), and your own bugs ("the cron deleted the wrong tenant")
  all want that row back.
- **A giant cleanup `DELETE`** on a big table holds row locks for the whole
  statement, bloats undo, stalls replicas, and makes every locking read that
  touches those rows wait for the full transaction. (Drill C2 measures this:
  reader p95 went from **3ms to 203ms** the moment one statement deleted 14k
  rows.)
- **Soft delete with `deleted_at`** quietly poisons the schema: every query
  must remember a WHERE clause, `UNIQUE` constraints stop meaning what the
  product thinks they mean, and the table never gets smaller.
- **The mirror-schema design** buys real recoverability, but the price is an
  operational contract most teams don't notice they signed: every schema
  migration must now be applied twice, forever, atomically-ish, across all
  "many big tables".

The interesting question is not "which is best" — it's *which failure you'd
rather own*. This lab makes you own each one for twenty minutes.

## 2. Architecture

Three isolated schema families, seeded with identical data from the same RNG
seed, so every measurement compares like with like:

```
                       ┌────────────────────────────────────────────┐
                       │                 lab10-mysql                │
   traffic.py a ──────►│  lab10_a         users ─ orders ─ items    │
                       │                  (deleted_at + index)      │
                       │                                            │
   traffic.py b ──────►│  lab10_b         users ─ orders ─ items    │
                       │                     │ move (1 txn)         │
                       │  lab10_b_deleted    ▼ same PK, no FK,      │
                       │                  users  orders  items      │
                       │                  + _deleted_at/_deleted_by │
                       │                                            │
   traffic.py c ──────►│  lab10_c         users ─ orders ─ items    │
                       │                     │ archiver.py          │
                       │  lab10_c_archive    ▼ (batched, resumable) │
                       │                  users  orders  items      │
                       └────────────────────────────────────────────┘
```

Every family has real FKs on the live tables (`orders.user_id → users.id`,
`order_items.order_id → orders.id`) because "many big tables" pain is mostly
*relationship* pain: a user delete is never one row.

The contract between components:

- `traffic.py <family>` journals every **acked** op to `traffic_<f>.jsonl`.
  A delete is journaled as a `delete_request` — traffic doesn't decide what
  deleting means; the strategy under test does.
- Strategies consume requests and append what they did to `outcome_<f>.jsonl`
  (`soft_deleted` / `moved` / `purged`).
- `verify.py` joins journal + outcomes + database and enforces the invariant:
  **every id lives in exactly one place** (live XOR mirror/archive, or
  provably purged), and no id may ever exist on both sides of a move.

## 3. Deep dive: three ways to not-delete a row

### 3.1 Why soft delete exists at all

Four forces push every growing system toward it: **restore** (user-facing
undelete is a product feature, not a DBA favor), **audit** (who deleted what,
when), **referential sanity** (hard-deleting a user either cascades through N
tables or leaves orphans), and **fear** (an `UPDATE` that sets a flag is
reversible; a `DELETE` is not). Note what's *not* on the list: performance.
Soft delete is never faster; it's a trade of disk and query complexity for
recoverability. And one force pushes back hard: **retention**. "We never
delete anything" is a liability sentence in a GDPR/PCI world — someone must
eventually really delete. Strategies A and B have no story for that; strategy
C's purge step *is* the story.

### 3.2 Strategy A: `deleted_at` — the two traps everyone falls into

**Trap 1 — the forgotten WHERE.** The filter is an *invariant distributed
across every query in the codebase*. Nothing enforces it. Drill A1 runs a
revenue report both ways on the same data:

```
revenue report WITHOUT filter:   1283319.07   <- silently includes deleted rows
revenue report WITH    filter:    895180.71
overcount: 397708.18 (44.4%) — no error, no warning, just a wrong number
```

No exception, no log line — a *plausible wrong number*, which is the worst
kind. Mitigations, in increasing order of seriousness: code review
convention → a view layer (`orders_live`) that bakes the filter in and
revoking direct table access → moving to strategy B/C where deleted rows
physically can't be selected.

**Trap 2 — resurrection vs UNIQUE.** `UNIQUE(email)` doesn't know about
`deleted_at`. Drill A2: soft-delete a user, then let them re-register:

```
re-registration FAILED: (1062, "Duplicate entry 'user2@example.com' for key 'users.uq_email'")
```

The product thinks the account is gone; the database disagrees. The drill then
executes the crudest working recovery — tombstone the dead row's value
(`email = CONCAT(email, '#deleted#', id)`) and watch the re-registration
succeed. The textbook
fix, `UNIQUE(email, deleted_at)`, has a MySQL-shaped hole: NULLs never
collide in a unique index, so **two live rows** with the same email and
`deleted_at = NULL` are both accepted — the constraint now fails in the
opposite, much worse direction. Workable fixes:

- a generated column: `alive TINYINT AS (IF(deleted_at IS NULL, 1, NULL))`
  with `UNIQUE(email, alive)` — live rows collide (1 = 1), deleted rows
  don't (NULL ≠ NULL);
- tombstoning the value on delete (`email = CONCAT(email, '#deleted#', id)`) —
  crude, loses the original address, works everywhere;
- PostgreSQL, where this is a one-liner:
  `CREATE UNIQUE INDEX ON users (email) WHERE deleted_at IS NULL` — partial
  indexes are the single biggest reason this strategy is more livable on
  Postgres. MySQL has no partial indexes; the generated column is the closest
  approximation.

**The quiet cost.** After "deleting" 30% of orders the table's data and index
bytes don't move at all — there was no DELETE. Every index still carries every
dead row; every range scan wades through them; the buffer pool caches
tombstones. On a 100M-row table with 30% tombstones you are paying for 30M
rows of pure liability on every query, forever.

### 3.3 Strategy B: the mirror schema — what the case-study team actually signed

The move itself is the easy part. The details that bite are:

**Ordering is forced by the FKs.** Mirror-inserts must go parent-first
(users → orders → items); live-deletes must go child-first (items → orders →
users). Restore is the exact reverse. Get it wrong and the transaction fails —
which is the *good* outcome.

**The id-snapshot race.** `strategy_b.py` locks the row family
(`SELECT ... FOR UPDATE`) and copies/deletes **by explicit id list**. The
tempting shortcut — `INSERT INTO mirror ... WHERE user_id=?` then
`DELETE ... WHERE user_id=?` — has a hole: traffic can insert a new order for
that user between the two statements, and the DELETE then destroys a row that
was never mirrored. Silent data loss, discovered only at restore time. The X
lock on the parent row closes it, because the FK check on the incoming insert
must wait.

**Mirror tables are not quite mirrors.** Ours keep the PK but drop FKs and
`UNIQUE(email)` — deliberately. Deleted data arrives in child-before-parent
commit order from cascades, and two generations of the same email must be
allowed to pile up. The mirror is a graveyard, not a database; constraints
are for the living.

**Why not triggers?** The obvious "fix" for app-level move code is a
`BEFORE DELETE` trigger per table that copies the row into the mirror — then
any `DELETE`, from any code path, gets mirrored for free. The costs are why
this lab builds the app-level version instead: the trigger fires *per row*
inside the deleting transaction (a big delete now pays a synchronous insert
per row, doubling its lock time); triggers are invisible in the codebase,
so the drift problem below gets *worse* — the ALTER now has three copies to
keep in sync (table, mirror, trigger body); and MySQL triggers don't fire
for FK-cascade deletes, exactly the multi-table case this lab is about. A
trigger turns the mirror from an application feature into a hidden database
behavior — same tax, less visibility.

**Schema drift is the tax.** The move uses positional
`INSERT ... SELECT t.*` — which is exactly how these systems get written.
Drill B1 ships an innocent feature-team ALTER:

```
a feature team ships: ALTER TABLE lab10_b.orders ADD COLUMN coupon ...
next delete FAILED: (1136, "Column count doesn't match value count at row 1")
```

Every delete in production now errors — and this is the *lucky* version. If
the mirror had coincidentally compatible column counts, values would silently
land in the wrong columns. From this day on, **every ALTER must ship twice,
forever, on every one of the "many big tables"** — and lab 02 taught us what
ALTERs on big tables cost. Multiply by table count; that's the real price
tag of this architecture.

**What you get for all that:** the cleanest restore in the lab. The live
schema stays truly lean (no tombstones, no filters, `UNIQUE` means unique),
and undelete is a reverse move that round-trips byte-identical:

```
checksum live-before=a8ec64b91b3e604e mirror=a8ec64b91b3e604e live-after=a8ec64b91b3e604e
```

### 3.4 Strategy C: flag now, move later — the production pattern

Separate the two jobs that A and B each try to do with one mechanism:

- **The user's delete** must be instant and safe → it's a flag update
  (p95 ≈ 12ms in the bench), same as A.
- **The table's hygiene** must not involve the user → a background archiver
  moves flagged rows out in small batches, pt-archiver style.

The archiver's crash-safety needs **no checkpoint table**: the predicate
(`deleted_at IS NOT NULL`) *is* the work queue. Each batch is one short
transaction — `SELECT ids FOR UPDATE → INSERT into archive → DELETE from live
→ COMMIT` — so `kill -9` at any instant either lands the whole batch or none
of it, and a restart just re-selects whatever is still flagged. The archive PK
equals the source PK with `ON DUPLICATE KEY UPDATE`, so even a re-copy can't
duplicate. Drill C1 SIGKILLs it mid-batch and proves totals intact, zero
overlap, zero rows lost after restart.

Tables drain child-first with a "no live children" guard on parents
(`NOT EXISTS(...)`), so FKs hold even while traffic keeps flagging new
families mid-drain.

The window where a row is flagged-but-not-yet-archived means readers still
need the `deleted_at IS NULL` filter — C inherits A's trap 1 (mitigate the
same way), but the window is minutes, not forever, so index bloat never
accumulates. And C is the only strategy with a **retention answer**: the
purge (`RETENTION_S`) against the archive is the actual compliance deadline —
the soft delete was just the user-visible half.

### 3.5 The road not taken: partitioning

If your deletes are *time-based* (expire everything older than 90 days),
none of the above is right: `PARTITION BY RANGE` on a date column turns
retention into `ALTER TABLE ... DROP PARTITION` — metadata-fast, no row
locks, no undo, no archiver. Why it isn't this lab's answer: MySQL requires
the partition key inside every unique key (goodbye clean `UNIQUE(email)`),
FKs are unsupported on partitioned tables entirely, and *entity* deletes
("this user, now") don't map to partitions — a user's rows are smeared
across all of them. Partition when deletes align with the partition axis
(logs, events, audit trails); use A/B/C when they don't. And retro-fitting
partitioning onto an existing 100M-row table is a lab-02-sized online
migration of its own.

## 4. Runbook (step by step)

```bash
make demo          # everything below, end-to-end (500k orders/family; ~10 min)
make demo SEED_ROWS=50000   # the 2-minute version
```

What it runs, in order — or run each step yourself:

```bash
make up install bootstrap seed          # 1. MySQL + 3 identical families
make traffic-start FAMILY=a            # 2. live writes + delete_requests (a, b, c)
make strategy-a                        # 3. A: consume requests + drills A1/A2
make strategy-b                        # 4. B: consume requests + restore round-trip
make strategy-c archiver-drain         # 5. C: flag instantly, then drain archiver
make traffic-stop FAMILY=a             # 6. (a, b, c)
make drill-drift                       # 7. drills, see below
make drill-kill-archiver
make drill-big-delete
RETENTION_S=0 make purge               # 8. really delete the archive
make verify                            # 9. exactly-once accounting
make bench                             # 10. the comparison table (resets world)
make clean                             # back to pristine
```

Expected tail of a run (50k seed):

```
09:22:48 INFO  [a] checked 764 journaled rows -> 0 violations
09:22:49 INFO  [b] checked 894 journaled rows -> 0 violations
09:22:50 INFO  [c] checked 1006 journaled rows -> 0 violations
09:22:50 INFO  verify: OK — every acked op is accounted for, every id lives in exactly one place

                                        A deleted_at        B mirror      C archiver
--------------------------------------------------------------------------------------
delete p50 (ms, user-facing)                     8.5            10.6             7.6
delete p95 (ms, user-facing)                    13.6            19.7            13.0
read p50 (ms, correct query)                    0.45            0.49            0.45
live schema size (MB)                           27.5            23.1            27.5
dead rows left in live table                     507               0               0
read p95 (ms, unfiltered = WRONG)               1.48               -            1.97
archiver drain (s, background)                     -               -             0.7
restore (verified by probe)            OK: flip flag  OK: reverse move   OK: copy back
```

### Failure drills

**Drill A1 — forgotten WHERE** (inside `make strategy-a`): the unfiltered
revenue report silently overcounts by every deleted order. Expected output in
§3.2.

**Drill A2 — resurrection** (inside `make strategy-a`): re-registering a
soft-deleted email fails with 1062. Expected output in §3.2.

**Drill B1 — schema drift** (`make drill-drift`): ALTER the live table,
"forget" the mirror, watch every delete fail with 1136; repair the mirror,
watch it recover. Expected output in §3.3.

**Drill C1 — kill the archiver** (`make drill-kill-archiver`): SIGKILL
mid-batch, assert zero overlap and intact totals, restart, drain to zero:

```
archiver killed with SIGKILL after 1.5s (pid 122824)
post-crash: no live/archive overlap, totals intact {'users': 4060, 'orders': 20222, ...}
after restart+drain: 0 flagged rows, totals intact
drill C1 PASSED — copy+delete in one transaction needs no checkpoint: the predicate is the work queue
```

**Drill C2 — one giant DELETE vs batches** (`make drill-big-delete`): equal
row counts, a concurrent locking reader sampling throughout, objective
assertion `batched p95 < giant p95`:

```
giant:   14303 rows in ONE transaction, 0.27s — locks held the whole time
batched: 14303 rows in 1000-row transactions, 1.45s wall (incl. pauses)
concurrent reader p95: giant=203.3ms  batched=3.0ms
drill C2 PASSED — the slow path is FASTER for everyone else
```

## 5. Production checklist — what changes at 100M rows and a pager

- [ ] **Batch sizes shrink, pauses grow.** The archiver's 500-row batches and
      100ms sleeps are laptop numbers. In production, size batches by *replica
      lag and lock-wait SLOs*, not throughput; make both knobs runtime-tunable
      and add a kill switch.
- [ ] **The archiver follows replica lag.** Every batch is replicated twice
      (insert + delete). Throttle on `Seconds_Behind_Source` (lab 02's loop)
      or the archiver will do to your replicas what the giant DELETE did to
      the reader in drill C2.
- [ ] **`deleted_at` needs an index that matches the archiver's query** —
      ours uses `idx_deleted_at`; at 100M rows a missing index turns each
      batch-select into a table scan under lock.
- [ ] **Strategy B at scale needs migration tooling, not discipline.** If you
      keep the mirror design, the "ALTER ships twice" rule must be enforced by
      the migration framework (one migration file fans out to both schemas),
      never by code review. Drill B1 is what a missed one looks like at 3am.
- [ ] **Restore is a product flow, test it like one.** B's reverse move can
      hit a re-registered email (1062 on restore). Decide now: block restore,
      or rename-on-restore, or reserve tombstoned values.
- [ ] **Purge is a legal deadline.** Retention comes from compliance, runs
      from the archive (never the live table), and needs monitoring for
      "oldest unpurged row age" — that's the metric an auditor asks about.
- [ ] **InnoDB never shrinks in place.** Even after C drains a big backlog,
      the live tablespace keeps its high-water mark; reclaiming disk is
      `OPTIMIZE TABLE`/`ALTER ... ENGINE=InnoDB` — a lab-02 online rebuild.
      Plan it, don't improvise it.
- [ ] **Watch history list length** (`SHOW ENGINE INNODB STATUS`) during any
      mass delete/archive: a long-running read transaction anywhere blocks
      purge everywhere, and undo grows until it wins.
- [ ] **Backups interact with deletes.** A soft-deleted row is in every
      backup; a purged row must eventually leave them too. If compliance says
      "gone", your binlog/backup retention is part of the answer.

## 6. Interview questions to answer without notes

1. The unfiltered revenue report returned a wrong number with no error. Name
   two structural (non-convention) mechanisms that make the forgotten-WHERE
   bug impossible, and the cost of each.
2. Why does `UNIQUE(email, deleted_at)` fail in MySQL in the *opposite*
   direction from the bug it tries to fix? Walk through the NULL semantics,
   then explain how the generated-column trick restores both properties.
3. In strategy B's move, why must the id sets be snapshotted `FOR UPDATE`
   before the copy? Describe the exact interleaving with a concurrent order
   insert that loses data without the lock, and why the FK check makes the
   lock sufficient.
4. Mirror tables here drop FKs and unique keys. Argue for and against —
   what breaks if the mirror keeps `UNIQUE(email)`? What do you lose without
   any constraints?
5. The archiver has no checkpoint table yet survives `kill -9` mid-batch.
   State the two properties that make this true, and give a code change that
   would silently break each one.
6. Why do the archiver's parent tables need the `NOT EXISTS` child guard even
   though flagging cascades in one transaction? Construct the failing
   sequence without it.
7. Drill C2's batched deletes took 5× longer in wall-clock time yet the
   drill claims they're "faster". Faster for whom, exactly — and which two
   InnoDB mechanisms make the giant statement expensive for everyone else?
8. Your product deletes users on request (GDPR) *and* expires events after
   90 days. Which mechanism from this lab does each requirement get, and why
   is one answer partitioning while the other can't be?
9. `verify.py` treats the journal, not the database, as ground truth. Why is
   "acked ops only" the right journaling rule — what false alarms and what
   real bugs would journaling *sent* ops produce?
10. You inherit the case-study system (mirror schema, 40 tables) and are
    asked to migrate to strategy C without downtime. Sketch the
    expand/contract plan (lab 09) — what runs in double-write mode, and what
    is the point of no return?

## 7. File map

```
docker-compose.yml            MySQL 8.0.43 (1G buffer pool), optional adminer UI
Makefile                      every step above; `make help`
sql/schema.sql                the 5 schemas / 15 tables, design notes in comments
scripts/common.py             config, connect(), journal/outcome helpers, percentiles
scripts/bootstrap.py          (re)create all schemas from schema.sql, idempotent
scripts/seed.py               identical seed per family (users:orders:items = 1:5:15)
scripts/traffic.py            per-family live writes + delete_request journal
scripts/strategy_a.py         A: cascade flagging + drills A1 (WHERE) / A2 (unique)
scripts/strategy_b.py         B: locked transactional move, restore, drift drill
scripts/archiver.py           C: flag mode, batched archiver, retention purge
scripts/drill_kill_archiver.py  SIGKILL mid-batch, prove exactly-once survives
scripts/drill_big_delete.py   giant vs batched DELETE with concurrent locking reader
scripts/verify.py             journal ⋈ outcomes ⋈ DB — exactly-once invariant
scripts/bench.py              reset world, same workload per family, comparison table
```
