# Lab 03 — CDC: Syncing Big Data Between Two Datasources (MySQL → Elasticsearch)

> Keep an Elasticsearch index continuously in sync with a live MySQL table —
> initial sync of existing rows plus every subsequent INSERT/UPDATE/**hard
> DELETE** — using Debezium + Kafka, a hand-written projector, and a
> reconciler that proves (and repairs) the sync. No app changes, no
> dual-writes, **provably zero data gap** across three systems.

## 1. The problem

The orders live in MySQL; search needs them in Elasticsearch. Every team
solves this at some point, and almost every team's first solution is the same
wrong one:

| Approach | Failure mode |
|---|---|
| **Dual-write from the app** (`INSERT` then `es.index()`) | The two writes are not atomic. ES write fails after MySQL commit → silent gap. Retry the ES write → maybe it was the *MySQL* write that failed → orphan. Two app instances race on the same row → ES ends up with the *older* version (no ordering). And none of this handles the 200k rows that already exist. |
| Dual-write + try/catch + queue for retries | You are now building a worse version of this lab: a change log without ordering, without a position, and coupled to every write path in the app. |
| **Periodic full re-sync** (cron `SELECT *` → bulk index) | Simple and correct-ish — but staleness = cron interval, deletes require full diffing, and at 100M rows the "periodic" job becomes a permanent background load. Keep this idea though: it comes back as *reconciliation* (§3.6). |
| Query-based incremental sync (`WHERE updated_at > ?`) | Lab 01's catch-up loop as a permanent service: clock skew margins forever, and **hard deletes are invisible**. |

The structural fix: **stop trying to write to two places.** The application
writes to MySQL, full stop. MySQL's binlog — the database's own ordered,
complete, crash-safe change record — becomes the event stream, and everything
downstream is a *projection* of it. That is Change Data Capture:

- **Debezium** (the industry-standard CDC engine, run inside Kafka Connect)
  snapshots the existing table, then tails the binlog — the productionized
  version of the applier we hand-wrote in Lab 02.
- **Kafka** buffers and orders the events per key, decoupling source from
  consumers (ES today; caches, analytics, lab 05's consumers tomorrow).
- **The projector** — the part you write — consumes events and applies them
  to ES *idempotently*, with offsets committed only after ES acknowledges.

## 2. Architecture

```
 app writes ──► MySQL (source of truth)
                  │ binlog (ROW, FULL images)
                  ▼
        Debezium MySQL connector          ┌── Kafka Connect keeps connector
        (inside Kafka Connect) ───────────┤   offsets + config in Kafka itself:
                  │                       └── kill it, it resumes exactly
                  │ snapshot (op:'r'), then stream (c/u/d)
                  ▼
        Kafka topic lab03.lab03.orders  (3 partitions, keyed by PK
                  │                      ⇒ total order PER ROW)
                  ▼
        projector.py  (consumer group, manual commits)
                  │ REPLACE-style upsert (_id = PK) / delete; commit AFTER ack
                  ▼
        Elasticsearch index `orders`  ◄── reconcile.py periodically diffs
                                          against MySQL and repairs drift
```

Lab 02 and this lab are the same idea at two lifetimes: lab 02 ran
binlog-replay for *minutes* to migrate a table and tore it down; CDC runs it
**forever** as infrastructure. The correctness arguments transfer almost
one-to-one — snapshot/stream fencepost, idempotent apply, deletes as events.

## 3. Deep dive: the six hard sub-problems

### 3.1 Why dual-write can't be patched (the two-generals of app code)

The app-level dual-write fails not because of bugs but because of physics:
two non-atomic writes to independent systems cannot be made atomic from the
outside. Every "fix" relocates the gap: retries need idempotency *and* a
persistent retry store (congratulations, you're building a queue);
transactions-then-publish has the same gap between commit and publish;
publish-then-commit inverts it. The only clean escapes are (a) read the
database's own log *after* commit — CDC, this lab — or (b) make the publish
part of the commit — the outbox pattern, which is Lab 05 territory. Know both;
they are the two right answers to one of the most common design questions.

### 3.2 Debezium anatomy: snapshot → stream, and the envelope

A big existing table needs two phases, and the fencepost between them is
where naive implementations lose data. Debezium's `initial` snapshot:

1. Grab a global read lock **just long enough** to read the current binlog
   position and schema (milliseconds — watch `traffic.log` while the
   connector registers: nothing even hiccups).
2. Release the lock; read all existing rows in a REPEATABLE READ transaction
   consistent with that position — emitted as events with `op: "r"`.
3. Stream the binlog from exactly the recorded position.

Same fencepost as Lab 02's "record position *before* backfill": overlap is
impossible to get wrong in one direction (replays are harmless, §3.4) and a
gap is impossible by construction.

Every event is an **envelope**: `{op: r|c|u|d, before, after, ts_ms, source}`
with the full row image in `after` (or `before` for deletes) — full images
because the binlog is configured `ROW`/`FULL`, the same contract Lab 02
depended on. Keys: the row's PK, `{"id": 42}`. Deletes additionally emit a
**tombstone** (null value) so Kafka log compaction can eventually drop the
key; consumers just skip it.

Operational notes worth knowing cold: connector state (binlog position,
config) lives in Kafka topics (`_connect_offsets`, ...), which is why Connect
can be killed and resume seamlessly (drill 2); schema changes are tracked in
a schema-history topic so the connector can parse binlog events for any
table version.

### 3.3 Ordering: what Kafka guarantees, what it doesn't, and what you need

Kafka guarantees order **within a partition** only. Debezium keys events by
primary key, and the topic partitioner hashes the key — so **all events for a
given row land in one partition, in commit order**. Different rows may be
consumed out of order relative to each other.

Is that enough? For a projection, yes — and proving it is a good interview
moment: ES docs are independent (no cross-document constraints), so applying
each row's event stream in order converges every doc to its source row.
Per-key order is exactly the guarantee required; total order is not.

What silently breaks it (all real incidents, all invisible until an update
pair races):

- Repartitioning by another field ("let's key by customer for locality") —
  two updates to one order can now land in different partitions and apply in
  reverse.
- A fan-out/enrichment hop that round-robins to workers.
- Consuming one partition with multiple threads for "throughput".

Scaling rule that follows: throughput scales by **adding partitions and
consumers** (Kafka assigns each partition to exactly one consumer in the
group), never by parallelizing within a partition. The lab uses 3 partitions
and 1 consumer; start a second projector and watch a rebalance split 2/1.

### 3.4 Delivery guarantees: the exactly-once illusion, built honestly

Every hop here is **at-least-once**: Debezium may re-emit events after a
crash (it commits its binlog position periodically), and the projector may
re-process after a rebalance or crash. Exactly-once *delivery* to an external
system is not a thing you can buy — Kafka's EOS covers Kafka-to-Kafka, not
Kafka-to-Elasticsearch. What you build instead:

**at-least-once transport + idempotent apply = effectively exactly-once.**

The projector's two halves of that bargain:

1. **Idempotent apply**: doc `_id` = row PK, whole-document upsert (the same
   "full row image, REPLACE semantics" as Lab 02's applier); deletes treat
   404 as success (already deleted on a previous attempt — fine).
   Re-applying any suffix of the event stream is a no-op.
2. **Commit-after-write**: `enable.auto.commit=false`, and offsets are
   committed **only after the ES bulk call is acknowledged**. Auto-commit
   (the default!) commits on a timer regardless of what your code did with
   the messages — crash between auto-commit and your ES write and events are
   *gone*, the one failure mode this whole lab exists to prevent. With
   manual commit-after-write, a crash anywhere replays a batch — see drill 1,
   where we `kill -9` the projector mid-stream and verification stays green.

One decision baked into the projector worth making explicit: on an ES bulk
failure it **stops** (offsets uncommitted) rather than skipping — for a
*projection*, halt-and-catch-up beats a dead-letter queue, because a skipped
event is permanent silent drift while a stall is a visible lag alert.
For non-replayable side effects (emails, payments) you'd want the DLQ +
idempotency keys instead — that's Lab 05.

### 3.5 The type traps (everyone hits these in the first week)

Debezium's JSON is not "just your row as JSON":

| MySQL type | What arrives in the event | Handling |
|---|---|---|
| `DECIMAL(12,2)` | **base64-encoded unscaled bytes** by default (`precise` mode) — `"E4Q="` where you expected `19.99` | `decimal.handling.mode=string` → `"19.99"`; parse in the consumer. (`double` is convenient but lossy — you migrated to DECIMAL in Lab 02 for a reason.) |
| `DATETIME(3)` | epoch **millis**, no timezone (`io.debezium.time.Timestamp`) | ES date field with `format: epoch_millis`. |
| `TIMESTAMP` | ISO-8601 **string in UTC** (`ZonedTimestamp`) — different from DATETIME! | Know which your schema uses. |
| `ENUM` | plain string | free. |
| `BIGINT UNSIGNED` | may exceed JSON-safe integers downstream | fine into ES `long`; beware JS consumers. |

The lab's `row_to_doc()` is deliberately tiny so these conversions are
visible instead of buried in a framework. The ES mapping uses
`dynamic: strict` so any field these traps mangle fails loudly at index time
instead of dynamic-mapping itself into a wrong type forever.

### 3.6 Reconciliation: trust, but verify (and repair)

Even a correct pipeline drifts in production: someone writes to ES directly,
a projector bug maps a field wrong for a week, Kafka retention expires during
a long outage, a fat-fingered index delete. CDC keeps the *stream* honest;
nothing keeps the *state* honest unless you check it.

`reconcile.py` is the check: chunked PK-range sweep comparing MySQL (source
of truth, always wins) against ES, classifying drift as **missing** (in
MySQL, not ES), **stale** (both, fields differ), **orphan** (in ES, not
MySQL — a lost delete), and `--repair` fixes all three from MySQL. Drill 3
sabotages ES on purpose and watches the reconciler put it back.

Two production notes: (a) run it against a live pipeline and rows changed
mid-sweep show up as false drift — real reconcilers re-check candidates
after a delay or compare `updated_at` watermarks before repairing; (b) the
same sweep against an *empty* index is your disaster-recovery rebuild, and
Debezium's incremental snapshots (signal table) are the productionized
version of exactly that.

## 4. Runbook (step by step)

```bash
cd labs/03-cdc-mysql-to-es

# 0. One command to see the whole story:
make demo

# --- or step by step: ---
make up install          # 1. MySQL + Kafka (KRaft) + Connect + ES  (ES on :9201)
make bootstrap           # 2. source table + ES index + 3-partition topic
make seed                # 3. 200k existing rows — the "big data" to sync
make traffic-start       # 4. live writes begin BEFORE the pipeline exists
make connector           # 5. Debezium: snapshot 200k rows, then stream binlog
make projector-start     # 6. Kafka -> ES projector (tail -f projector.log)
make status              # 7. connector state + consumer lag
make traffic-stop
make verify              # 8. ✅ prove zero gap end-to-end (waits for drain)
make reconcile           # 9. full MySQL-vs-ES diff: no drift
```

What you should observe (numbers from this lab's demo run):

- Traffic starts *before* the connector exists — the pipeline syncs a table
  that is already big and busy, which is the actual problem.
- `projector.log`: a burst of `op:'r'` snapshot events, then live c/u/d
  events with no seam.
- `make verify`: CHECK 0 shows ES climbing to the MySQL count as the
  snapshot drains, then `Converged: MySQL == ES == 200710 rows`, then journal
  replay proves every acked MySQL write (deletes included) reached ES.
- `make reconcile`: `Scanned 200710 rows ... 0 missing, 0 stale, 0 orphans`
  in ~22s.

### Failure drills

```bash
# Drill 1: kill -9 the projector mid-stream (uncommitted batch in flight),
# restart it. The batch replays; idempotent upserts absorb the duplicates.
make traffic-start
kill -9 $(cat projector.pid)
sleep 10 && make projector-start
make traffic-stop && make verify          # still green

# Drill 2: kill Kafka Connect mid-stream. Its offsets live in Kafka, so on
# restart Debezium resumes from its last committed binlog position.
make traffic-start
docker kill lab03-connect && sleep 15
docker start lab03-connect
make traffic-stop && make verify          # still green

# Drill 3: sabotage ES directly (the pipeline can't see this — no binlog
# event ever happens: one deleted doc, one hand-corrupted doc, one orphan).
curl -s -XDELETE "localhost:9201/orders/_doc/42"
curl -s -XPOST "localhost:9201/orders/_update/100" -H 'Content-Type: application/json' \
     -d '{"doc":{"status":"cancelled","note":"corrupted by hand"}}'
curl -s -XPOST "localhost:9201/orders/_doc/99999999" -H 'Content-Type: application/json' \
     -d '{"id":99999999,"customer_id":1,"status":"paid","amount":1,"note":"orphan","created_at":0,"updated_at":0}'
make reconcile                            # ❌ 1 missing, 1 stale, 1 orphan
RECONCILE_ARGS=--repair make reconcile    # repaired from source of truth
make reconcile                            # ✅ clean

# Drill 4: THE idempotency proof — rewind the consumer group to offset 0 and
# re-project the ENTIRE topic over the live index. Observed: 203,682 events
# replayed in 75s, verification still green. If your projector can't survive
# this, it isn't idempotent and §3.4's guarantee is fiction.
make projector-stop
docker exec lab03-kafka /opt/kafka/bin/kafka-consumer-groups.sh \
    --bootstrap-server localhost:9092 --group es-projector \
    --topic lab03.lab03.orders --reset-offsets --to-earliest --execute
make projector-start
make verify                               # ✅ still green
```

A bug this lab caught in itself, preserved as a lesson: the first version of
the reconciler swept MySQL's PK range plus one chunk — and drill 3's orphan
at id=99999999 sailed straight past the sweep, undetected. The fix (two
unbounded range queries: anything in ES outside MySQL's id span is an orphan
by definition) is in `reconcile.py`. Moral: **your verifier needs failure
drills too** — a reconciler that has never caught a planted defect is
untested infrastructure.

## 5. Production checklist

Everything that changes between this lab and production:

- [ ] **Schema registry + Avro/Protobuf**, not schemaless JSON: half the
      payload size, and schema evolution stops being "hope the consumer
      parses it".
- [ ] **Snapshot mode for 100M+ rows**: `initial` locks trivially but copies
      for hours; know `snapshot.mode` options and **incremental snapshots**
      (signal table) for resumable, chunked snapshots that interleave with
      streaming.
- [ ] **Binlog retention vs downtime**: if a consumer or connector is down
      longer than `binlog_expire_logs_seconds` (or Kafka topic retention),
      you re-snapshot. Size retention to your worst credible outage.
- [ ] **Monitoring**: consumer lag (`kafka-consumer-groups --describe`),
      connector task state (alert on FAILED — it does not self-heal),
      Debezium's `MilliSecondsBehindSource`, and end-to-end freshness
      (`max(updated_at)` in ES vs MySQL).
- [ ] **Topic partitions ≥ target consumer parallelism** — you can add
      partitions later but existing keys re-hash (a controlled migration of
      its own). Overprovision modestly up front.
- [ ] **Log compaction** on CDC topics (+ tombstones on) if consumers may
      bootstrap from the topic instead of a fresh snapshot.
- [ ] **DDL discipline**: Debezium tracks schema changes, but your *sink*
      does not — an added column flows into events immediately while the ES
      mapping (`dynamic: strict`) rejects it. Migration order: sink mapping
      first, then source DDL (expand/contract — Lab 04's subject).
- [ ] **Reconcile on a schedule**, not after incidents. Weekly full sweep +
      alert on drift > 0 catches the bug you don't know about yet.
- [ ] **One consumer group per projection**; never share groups across
      services (offsets are per-group — sharing splits the stream).
- [ ] **Aliases on the ES side** (Lab 01!): the projector writing through an
      alias is what makes reindexing the projection a non-event later.

## 6. Interview questions to answer without notes

1. Walk through the exact failure sequences that make app-level dual-write
   unfixable, and name the two structural escapes (CDC, outbox). When is the
   outbox the *better* answer?
2. How does Debezium hand off from snapshot to streaming without a gap or
   overlap? Where have you seen that fencepost pattern before (Lab 02)?
3. Kafka only orders within a partition. Prove that per-PK ordering is
   sufficient for a projection — and give a downstream requirement for which
   it would *not* be sufficient.
4. Auto-commit is on and your service crashed. Describe the exact window
   that loses events, and the window that duplicates them. Now explain why
   commit-after-write plus idempotent apply fixes both.
5. Why does the projector treat a delete's 404 as success? What invariant
   would a "strict" 404-is-error projector actually be violating?
6. Your DECIMAL column arrives as `"E4Q="`. What happened, what are the
   three handling modes, and their trade-offs?
7. Kafka retention expired while your projector was down for three days.
   What are your options, in order of preference?
8. Design the reconciler for 100M rows: how do you chunk, how do you avoid
   false positives against a live pipeline, and who wins conflicts?

## 7. File map

```
docker-compose.yml         MySQL 8.0 (ROW/FULL binlog) + Kafka 3.7 KRaft + Debezium Connect 2.7 + ES 8.14
sql/schema.sql             source-of-truth orders table (DECIMAL already fixed — thanks, lab 02)
mappings/orders.json       ES index (dynamic:strict, epoch_millis dates, scaled_float money)
connectors/orders-source.json  Debezium config: snapshot initial, JSON w/o schemas, decimal=string
scripts/common.py          config, clients, row->doc conversion (the type traps, visible)
scripts/bootstrap.py       source table + ES index + 3-partition topic
scripts/seed.py            bulk load BEFORE the pipeline exists (that's the point)
scripts/traffic.py         live INSERT/UPDATE/hard-DELETE against MySQL only + journal
scripts/connector.py       register connector via Connect REST, wait for RUNNING
scripts/projector.py       the consumer: batching, idempotent bulk, commit-after-write
scripts/verify.py          converge-wait + journal replay + 3-way counts + sampling
scripts/reconcile.py       chunked MySQL-vs-ES diff: missing/stale/orphans, --repair
```
