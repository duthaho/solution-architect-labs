# Lab 02 — MySQL Online Migration of a Big Table (gh-ost Style)

> ALTER a table with millions of rows while it takes continuous INSERTs,
> UPDATEs and **hard DELETEs**, with a **~1-second write pause**, zero errors
> surfaced to the application, and **provably zero data loss** — then verify
> it, then roll it back.

## 1. The problem

The `orders` table stores money in `FLOAT` (someone's 2019 sin), and the app
now needs an index the table doesn't have. The fix is one line of DDL:

```sql
ALTER TABLE orders
    MODIFY amount DECIMAL(12,2) NOT NULL,
    ADD COLUMN currency CHAR(3) NOT NULL DEFAULT 'USD',
    ADD KEY idx_customer_created (customer_id, created_at);
```

On a laptop, that runs in a blink. On 100M+ rows under traffic, that one line
is an incident. MySQL picks the *most* expensive algorithm any clause needs —
and a column **type change** forces the worst one:

| ALTER operation | Best algorithm | Cost on a big live table |
|---|---|---|
| `ADD COLUMN` (with default, 8.0+) | INSTANT | metadata only — just run it |
| `ADD INDEX` | INPLACE | full table read; no write block, but replicas apply it **single-threaded** → hours of replication lag |
| `MODIFY <type>` (FLOAT→DECIMAL) | **COPY** | rebuilds the whole table **holding a shared lock: writes blocked for the duration** |

So the honest failure modes:

| Approach | Failure mode |
|---|---|
| Just run the ALTER | `COPY` algorithm: writes blocked for the whole rebuild. On 100M rows: hours. |
| Run it "at night" | The rebuild also needs ~2× disk, saturates I/O, and there is no pause/abort — at 90% you can only kill it and eat the rollback. |
| `pt-online-schema-change` (triggers) | Works, but triggers add synchronous overhead to every production write, can't be throttled, and deadlock under load. See §3.2. |
| Split into 3 ALTERs, use INSTANT where possible | Right instinct! But `MODIFY` still forces COPY, and `ADD INDEX` still lags every replica. You still need this lab for two of the three clauses. |

The real solution — pioneered by Facebook's OSC, refined by GitHub's
[gh-ost](https://github.com/github/gh-ost) — is to **build a shadow table and
migrate the data around the DDL** instead of asking the DDL to be online.
This lab implements it from scratch, then **proves** it lost nothing.

## 2. Architecture

```
                       ┌──────────────────────────────────────────────┐
        app writes ───►│  orders (v1)                                 │
                       └───────┬──────────────────────────┬───────────┘
                               │                          │
                               │ row-based binlog         │ chunked backfill
                               │ (INSERT/UPDATE/DELETE    │ INSERT IGNORE ... SELECT
                               │  as row events)          │ ... FOR SHARE
                               ▼                          ▼
                       ┌──────────────────────────────────────────────┐
   applier: REPLACE /  │  _orders_gst (v2 schema, built empty,        │
   DELETE, in order ──►│  ALTERed while empty = instant)              │
                       └──────────────────────────────────────────────┘

   cutover:  LOCK TABLES orders WRITE, _orders_gst WRITE   -- writes queue
             marker row -> drain binlog to marker           -- ghost == source
             RENAME TABLE orders TO _orders_old,            -- atomic swap
                          _orders_gst TO orders
             UNLOCK TABLES                                  -- queued writes land on v2
```

Two data paths run **concurrently with no coordination**, and their conflict
rules make that safe:

- **Backfill** copies rows that existed at start: `INSERT IGNORE` — *never
  overwrites*.
- **Binlog applier** replays live changes: `REPLACE` / `DELETE` — *always
  overwrites*.

For any given row, binlog data is newer than (or equal to) backfill data.
"Binlog always wins, backfill never fights" is the entire consistency model —
one sentence, worth an interview on its own. (Plus one race that needs a
lock; see §3.4.)

The `RENAME TABLE` swap is the exact analogue of Lab 01's atomic alias swap:
one atomic, server-side switch that no client can observe half-done.

## 3. Deep dive: the six hard sub-problems

### 3.1 Why the binlog, and not `updated_at` catch-up like Lab 01?

Lab 01 caught up by re-copying `updated_at >= cursor`. That worked, but paid
three taxes: an overlap margin for clock skew, convergence heuristics, and —
worst — **hard deletes are invisible** (a deleted doc leaves nothing to copy),
forcing soft-delete discipline on the whole application.

The binlog is the database's own replication stream: every committed row
change, **in commit order**, with **full row images** (`binlog_format=ROW`,
`binlog_row_image=FULL`), including DELETEs as first-class events. Subscribing
to it (this lab uses `mysql-replication`, speaking the same protocol as a
replica) gives exact, ordered, gap-free change capture:

| | Lab 01 (timestamp catch-up) | Lab 02 (binlog CDC) |
|---|---|---|
| Deletes | invisible — needs soft deletes | first-class events |
| Clock skew | overlap margin heuristic | irrelevant — binlog position, not wall time |
| Convergence | "pass got fast enough" | exact: drain to a marker event |
| Change granularity | whole doc re-copy | per-row event, in commit order |
| This is why | — | CDC (Lab 03) is the graduation of this idea |

This lab's traffic generator does **5% hard DELETEs** on purpose — the case
Lab 01 had to design away — and `verify.py` proves they propagated (a
resurrected row fails verification).

### 3.2 Why binlog and not triggers (gh-ost vs pt-osc)?

`pt-online-schema-change` solves change capture with three triggers on the
source table mirroring every write into the ghost. It works, and it's the
right tool when you can't read the binlog. The trade-offs:

| | Triggers (pt-osc) | Binlog (gh-ost, this lab) |
|---|---|---|
| Write path overhead | every app write executes trigger DML **synchronously, in the same transaction** | zero — capture is async, off the write path |
| Throttleable? | no — triggers fire or they don't | yes — the applier is just a client; pause it |
| Lock/deadlock surface | triggers + app compete for ghost-table locks | none on the write path |
| Failure mode | remove triggers = lose changes | applier restarts from a binlog position |
| Requirements | none special | ROW binlog, replication privileges |

The deep reason gh-ost won: **triggers couple the migration to the production
write path**; the binlog decouples it. Everything else follows from that.

### 3.3 Chunked backfill — copying without melting the server

One `INSERT INTO ghost SELECT * FROM orders` would be a single hours-long
transaction: undo log bloat, replication stall, and a lock footprint that
grows with the table. Instead, `migrate.py` copies **2000-row chunks**, each
its own transaction:

- **Chunk boundaries by PK probe** (`SELECT id ... ORDER BY id LIMIT 1999,1`),
  not `id + 2000` arithmetic — deletes leave gaps, and range arithmetic would
  produce empty or lopsided chunks. gh-ost does exactly this.
- **`INSERT IGNORE`** — if the applier already wrote a newer version of the
  row, the backfill must lose. (§2's conflict rule.)
- **`FOR SHARE`** on the chunk SELECT — see §3.4; this is load-bearing, not decor.
- **Backpressure**: if applier lag exceeds 3s, the backfill pauses. A backfill
  that outruns the applier doesn't finish faster — it just moves the waiting
  into the cutover window.
- **Throttle knob** (`--chunk-sleep-ms`): in production you *will* need it;
  watch replica lag and p99 while the backfill runs.

Rows inserted after the backfill snapshot's `MAX(id)` are never backfilled at
all — they arrive purely via the binlog stream. The two paths meet exactly
because the binlog position was recorded **before** the backfill read anything:
every commit is either ≤ position (visible to the backfill's reads) or >
position (streamed). Overlap is harmless; a gap is impossible.

### 3.4 The delete race — why `FOR SHARE` is load-bearing

The one interleaving where "binlog wins" breaks down without help:

```
t1  backfill:  SELECT rows 1000-2999 from orders   (row 1500 included)
t2  app:       DELETE FROM orders WHERE id=1500    (commits)
t3  applier:   applies DELETE to ghost             (no-op — 1500 not there yet)
t4  backfill:  INSERT IGNOREs its stale copy       -> row 1500 RESURRECTED in ghost
```

The DELETE event was consumed *before* the stale insert landed, so nothing
ever deletes it again. This is the mirror image of Lab 01's deleted-doc
problem — CDC captures the delete, but a concurrent copier can still lose the
race against it.

`FOR SHARE` on the backfill SELECT closes it: the chunk read takes shared
locks, so the DELETE **cannot commit until the chunk's transaction commits**.
That forces the ordering `insert-into-ghost commits < delete commits < delete
binlog event`, and since the applier processes events in order, the DELETE is
applied *after* the row exists in the ghost — and removes it. Correct again.

Cost: writes to a chunk's rows stall for the milliseconds that chunk is being
copied. That's the price of correctness, and why chunks must be small.

Related subtlety, same family: the applier writes `REPLACE`, which is
DELETE+INSERT — any ghost-only column is reset to its **default** on every
replayed write. Fine for `currency CHAR(3) DEFAULT 'USD'` (constant default,
no backfilled variance), but if your new column is populated by a backfill
expression, REPLACE-based replay silently reverts it. gh-ost's answer, and
ours: migration-added columns must be default-filled during migration;
populate them *after* cutover.

### 3.5 Cutover — the atomic rename, and why gh-ost needs a "dance"

Same theorem as Lab 01: with async change capture you cannot have all three
of {no write pause, no app dual-write, zero loss}. This lab blocks writes —
but note how much better the ergonomics are than Elasticsearch's:

> **ES rejects, MySQL queues.** ES returns `cluster_block_exception` and the
> *client* must implement retry-with-backoff. MySQL's `LOCK TABLES ... WRITE`
> makes incoming statements **wait in the MDL queue** and proceed when the
> lock releases. The app needs *no special code* — it observes a brief
> latency blip. In this lab's demo run the measured block window was
> **0.09s**: so short that no write even crossed the traffic generator's
> 0.5s warning threshold. Zero errors, zero retries, zero lost writes.
> (The flip side: the lock queue also holds **reads** — see the checklist on
> `lock_wait_timeout` and MDL pileups.)

The sequence, executed **inside the applier thread** — the session that holds
the locks must be the one that applies the final events and renames:

1. `LOCK TABLES orders WRITE, _orders_gst WRITE` — app writes start queueing.
2. Insert a **marker row** into the changelog table via a *second* connection
   (the locked session may not touch unlocked tables; the changelog is not
   locked, so app-invisible writes still flow).
3. Keep draining the stream. Binlog order guarantees every `orders` event
   committed before the lock precedes the marker. **Marker seen ⇒ ghost ≡
   source.** No heuristics — this is what "drained" means, exactly.
4. Carry over the `AUTO_INCREMENT` counter (`CREATE TABLE LIKE` doesn't, and
   if the highest-id rows were deleted mid-migration the new table would
   re-issue their ids — a classic gh-ost war story).
5. `RENAME TABLE orders TO _orders_old, _orders_gst TO orders` — **atomic**.
6. `UNLOCK TABLES` — queued writes land on the new schema.

Step 5 is legal only because **MySQL ≥ 8.0.13** allows `RENAME TABLE` under
`LOCK TABLES` when you hold WRITE locks. On 5.7, `RENAME` was forbidden under
any table lock — which is exactly why gh-ost invented its two-connection
cutover dance: connection A locks the table and holds a *sentinel* table
lock; connection B issues a `RENAME` that **blocks on A's lock**; A drains
the binlog, then drops the sentinel and unlocks, and MySQL's MDL queue
priorities guarantee the queued RENAME wins over queued INSERTs. Elegant,
subtle, and obsolete on modern MySQL — `migrate.py`'s preflight checks the
version and tells you when you'd need the dance instead.

Two production-grade details in the lock step:

- `lock_wait_timeout = 10` on the cutover session, **with retries**. If a
  long-running SELECT holds an MDL on `orders`, our lock request queues behind
  it — and *every subsequent app write queues behind us* (MDL is a fair
  queue). A short timeout turns a would-be pileup into a failed attempt;
  gh-ost calls this cut-over retry.
- The block window includes everything between lock and unlock. Lab 01
  learned this as "orchestration overhead inflates the window" (a lazy 3s
  poll cost 6.66s); here the drain is event-driven, so the window is the
  final events + two statements — about a second.

### 3.6 Verify, or it didn't happen

Same doctrine as Lab 01, now with deletes to prove too. The traffic generator
journals every **acknowledged** write (`rowcount > 0` — an UPDATE matching
zero rows because its target was already deleted is a no-op, not a claim).
`verify.py` then:

1. **Journal replay** — computes last-op-wins expected state per row; asserts
   updated rows exist with journaled values and **deleted rows are absent**.
   A resurrected row (§3.4's race) fails here. This tests the user-visible
   contract: *acked means durable*.
2. **Count reconciliation** — `seeded + surviving inserts − deletes == COUNT(*)`.
3. **Content sampling** — rows untouched since cutover, field-by-field between
   `_orders_old` and `orders`, catching conversion corruption (`FLOAT →
   DECIMAL(12,2)` compared with 0.01 tolerance — v1's float 19.99 was really
   19.9899997711...; that imprecision is *why* we migrated).

## 4. Runbook (step by step)

```bash
cd labs/02-mysql-online-migration

# 0. One command to see the whole story:
make demo

# --- or step by step: ---
make up install          # 1. start MySQL 8.0 (ROW binlog, FULL row image + metadata)
make bootstrap           # 2. create v1 orders table + changelog table
make seed                # 3. load 500k rows (SEED_ROWS=5000000 make seed for real pain)
make traffic-start       # 4. live INSERT/UPDATE/DELETE begins (tail -f traffic.log)
make migrate             # 5. the migration — watch every phase log
make traffic-stop        # 6. stop traffic
make verify              # 7. ✅ prove zero data gap, deletes included
```

What you should observe during `make migrate`:

- Backfill throughput with live applier lag in the same log line — the two
  concurrent paths of §2, visibly coexisting (observed: ~18k rows/s backfill
  with applier lag steady at 10–30ms).
- Convergence by **lag**, not row count (the Lab 01 lesson, resurfacing).
- `Cutover complete: write-block window 0.09s` — and `traffic.log` showing
  **zero errors and zero warnings**: at this write rate the queue-and-proceed
  cutover is invisible to the app. Crank `SEED_ROWS` and the traffic rate to
  watch the window grow with the final drain.

### Failure drills

```bash
# Drill 1: rollback after cutover — replay post-cutover binlog into _orders_old,
# then the same lock/drain/rename dance in reverse. Run with traffic still on.
# (Observed: 942 post-cutover events replayed, reverse block window 0.54s,
# one app write delayed 0.54s, zero lost.)
make traffic-start
make rollback
make traffic-stop
make verify              # journal replay still passes against v1

# Drill 2: kill -9 the migration mid-backfill. Restart is from scratch —
# and that's the correct production answer too (state is one binlog position
# + an idempotent copy; a partial ghost is worthless, just cheap to rebuild):
make migrate &
sleep 15 && pkill -9 -f scripts/migrate.py
docker exec lab02-mysql mysql -uroot -plab lab02 -e "DROP TABLE _orders_gst"
make migrate             # completes normally; verify stays green

# Drill 3: non-convergence — crank traffic (drop the sleep in traffic.py),
# watch backfill backpressure kick in, then the converge phase time out and
# abort rather than cutting over with a lagging applier.
```

## 5. Production checklist

Everything that changes between this lab and 100M rows with a pager:

- [ ] **Use gh-ost itself** (or pt-osc/Spirit). This lab is the *understanding*;
      the tools have a decade of edge cases — use them, but now you can read
      their logs and debug their stalls.
- [ ] **Disk**: ~2× the table size free (old + ghost + binlog growth during
      the copy). Check before, not during.
- [ ] **Binlog retention**: the migration replays from a position captured at
      start; if `binlog_expire_logs_seconds` purges it mid-run (or mid-
      rollback-window), you restart from scratch. Size retention to migration
      duration + soak.
- [ ] **Run the applier against a replica** to read (gh-ost's default): zero
      binlog-read load on the primary; only backfill chunks and applied events
      touch it.
- [ ] **Throttle on replica lag**, not vibes: gh-ost pauses when replicas fall
      behind. Our lab's heartbeat-lag backpressure is the same mechanism.
- [ ] **FKs and triggers**: preflight refuses them, like gh-ost. FK children
      keep pointing at the *old* table after rename; triggers don't follow the
      rename either. Migrating FK-heavy schemas is its own project.
- [ ] **`lock_wait_timeout` + cutover retries**: a long-running query at
      cutover time must fail *your* lock attempt, not pile the whole write
      queue behind it. Schedule cutover away from batch jobs / backups.
- [ ] **AUTO_INCREMENT carry-over** (done here under the lock) — verify it;
      id reuse after cutover corrupts anything holding old ids.
- [ ] **Rollback window is a schema decision**: rolling back a widening
      migration discards new-column data (§rollback.py). Decide *before*
      cutover how long v2 must soak before you delete `_orders_old`.
- [ ] **Ghost-only columns must be default-filled during migration** (§3.4's
      REPLACE subtlety); backfill real values after cutover.
- [ ] **Keep `_orders_old`** until verification passes and a soak period
      (24–72h) elapses. Disk is cheaper than data loss.
- [ ] **Automate the whole runbook** — scripts, not shell history. A migration
      you can't rerun identically is a migration you can't roll forward.

## 6. Interview questions to answer without notes

1. Why does `MODIFY COLUMN` force `ALGORITHM=COPY` while `ADD COLUMN` can be
   INSTANT? What does `ADD INDEX` do to your replicas even though it's INPLACE?
2. State the backfill/applier conflict rules (`INSERT IGNORE` vs `REPLACE`)
   and prove they're correct for a row that is inserted, updated twice, and
   deleted during the backfill.
3. Walk through the delete-resurrection race and explain exactly how
   `FOR SHARE` changes the commit ordering to close it.
4. Why must the marker row be inserted *after* `LOCK TABLES` is granted, and
   through a *different* connection? What breaks if either is violated?
5. Why does gh-ost need its two-connection cutover dance on MySQL 5.7, and
   what changed in 8.0.13 that makes the simple version safe?
6. Your cutover attempt times out on `LOCK TABLES` twice. What is happening
   in the MDL queue, what is it doing to app traffic while you wait, and what
   do you change before attempt three?
7. Compare timestamp catch-up (Lab 01) and binlog replay (this lab) for:
   deletes, clock skew, convergence detection, and replaying into a *older*
   schema (rollback). Which would you pick for a system with soft deletes
   already in place, and why might you still choose the binlog?
8. After cutover you discover the new `currency` column is `'USD'` even for
   rows your backfill script had set to `'EUR'` pre-cutover. What happened?

## 7. File map

```
docker-compose.yml     MySQL 8.0, ROW binlog + FULL row image/metadata (+ adminer under --profile ui)
sql/v1.sql             original schema (amount FLOAT, missing index)
sql/v2_alter.sql       the migration DDL (applied to the empty ghost table)
scripts/common.py      config, connections, schema introspection helpers
scripts/bootstrap.py   create v1 table + changelog (heartbeat/marker) table
scripts/seed.py        batched bulk load
scripts/traffic.py     live INSERT/UPDATE/hard-DELETE + acked-write journal
scripts/applier.py     binlog streaming applier + atomic cutover state machine
scripts/migrate.py     the orchestrator: ghost -> stream -> backfill -> converge -> cutover
scripts/verify.py      journal replay + counts + old-vs-new sampling
scripts/rollback.py    reverse replay from saved binlog position + rename back
```
