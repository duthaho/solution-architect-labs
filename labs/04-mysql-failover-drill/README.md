# Lab 04 — MySQL Failover Drill: Measure the Data-Loss Window

Labs 01–03 are about *moving* data safely. This lab is about *losing* it — on
purpose, in a controlled way, so you know exactly what happens before the pager
teaches you.

`make demo` kills the primary while a writer is committing rows, promotes a
replica by GTID, and prints:

```
acked but LOST after failover:   172 rows   <-- RPO
```

Then it repeats the **exact same crash** with lossless semi-sync replication
and prints `0`.

## 1. The problem

"The primary is gone" is not an edge case. It's a scheduled event: hardware
dies, kernels panic, AZs vanish, and every minor-version upgrade is a
failover you run on purpose. The questions that matter are quantitative:

- **RPO** (recovery point objective): how many *acknowledged* commits can
  vanish? Not "sent" — *acknowledged*. The database said "durable", the app
  released the user's money based on that answer.
- **RTO** (recovery time objective): how long are writes down?

Most teams answer both with adjectives ("basically nothing", "a few seconds").
This lab answers with integers, because the writer keeps a **journal**: a line
per commit, appended *only after MySQL acks it*. After the failover we diff the
journal against the new primary. Every acked seq that's missing is a broken
promise, counted.

## 2. Why the naive approach fails

The naive failover is: primary died → promote whichever replica → point the
app at it → done. Four distinct disasters hide in there, one drill each:

1. **Async replication loses acked commits** (drill 1). The primary acks after
   its own fsync; replicas receive the binlog *later*. Crash in that gap and
   the acked tail exists only on a corpse.
2. **Wrong candidate** (manual drill 5). Replicas are not equally caught up.
   Promote the laggard and you lose data *that survived on the other replica* —
   a fully self-inflicted RPO.
3. **The zombie** (drill 4). SIGKILL doesn't erase the primary's disk. It
   reboots, still writable (`read_only=OFF` is its persisted config — it *was*
   the primary), and any client with a cached address happily keeps writing to
   it. Now two nodes both believe they're the source of truth: split-brain.
4. **"Semi-sync is on" is not a durability guarantee** (drill 3). Semi-sync
   silently degrades to async after `rpl_semi_sync_source_timeout`. If you
   don't monitor `Rpl_semi_sync_source_status`, you have async replication
   with extra steps and better vibes.

## 3. Concepts

### 3.1 Async vs semi-sync, AFTER_SYNC vs AFTER_COMMIT

**Async** (MySQL default): commit = binlog fsync + engine commit on the
primary, ack the client, *then* ship binlog to replicas whenever. The
acked-but-unshipped window is your RPO, and it's nonzero by design.

**Semi-sync, AFTER_SYNC** (aka "lossless", 5.7+): the primary writes the
transaction to its binlog, then **waits for ≥1 replica to ack receipt before
committing to the engine and acking the client**. If the primary dies
mid-wait, the client never got an ack — so nothing that was promised is lost.
The transaction either exists on a replica or was never acknowledged. RPO = 0
for acked writes.

**AFTER_COMMIT** (the pre-5.7 behavior, still configurable): the primary
commits *first*, then waits for the replica ack, then acks the client. Crash
in that window and the transaction is committed locally — visible to other
sessions reading the primary — but on no replica. Failover loses a
transaction other sessions may have already *seen and acted on*. Phantom
durability. This lab pins `AFTER_SYNC`; know the difference cold.

Semi-sync's price is one replica network round-trip *inside* every commit.
Measured here (same host, so this is the floor): avg commit 8.0ms → 12.0ms,
p95 12.7ms → 39.0ms. Cross-AZ, budget +1–3ms avg and a fatter tail.

### 3.2 GTID mechanics: retrieved vs executed

Every transaction gets a globally unique id: `server_uuid:seqno`. Two sets per
node tell you everything during an incident:

- `gtid_executed` — what this node has **applied**.
- `Retrieved_Gtid_Set` — what its IO thread has **received** into the relay log.

Received ⊇ applied while the SQL thread catches up. **Promotion compares
received, not applied**: a replica that received everything but applied half
has lost nothing — it just needs to drain its relay log before taking writes.
GTID algebra (`GTID_SUBTRACT`, `WAIT_FOR_EXECUTED_GTID_SET`) turns "who is
most caught up?" from a guess into set arithmetic.

### 3.3 Errant transactions

An **errant transaction** exists on some node but not on the current primary
lineage — `GTID_SUBTRACT(node, primary) ≠ ∅`. It's the fingerprint of writes
that happened where they shouldn't have: on a zombie, or on a laggard promoted
past a better replica. In this lab's zombie drill the errant set was
`...:1257-1490` — 234 transactions: 184 acked-but-lost rows that died with the
old primary **plus** 50 rows a stale writer pushed into the zombie after it
rebooted. The errant set *is* your incident report.

### 3.4 Fencing

A dead primary must be *made incapable* of taking writes before you trust the
new topology — that's fencing (production version: STONITH, killing its
network/power, revoking its VIP). Here: `SET PERSIST super_read_only=ON`.
`super_read_only` refuses writes even from root; `PERSIST` survives restart.
Replicas in this lab are *always* persisted read-only — a replica that reboots
must come back fenced, not writable. The zombie drill exists because the old
primary, by definition, has the opposite persisted config.

## 4. Architecture

```
                          ┌──────────────────────────┐
 traffic.py (writer) ─────►  router.json (who is primary)
   │ journals every        └──────────────────────────┘
   │ ACKED commit                  │ re-read on every reconnect
   ▼ journal.jsonl                 ▼
┌──────────────┐   async/semisync  ┌───────────────┐
│ primary :3307│ ─────────────────►│ replica1 :3308│
│              │ ─────────────────►│ replica2 :3309│
└──────────────┘    GTID auto-pos  └───────────────┘
```

- **3× MySQL 8.0**, GTID everywhere, `log_replica_updates` (every node keeps a
  full binlog — required for promotion and rebuilds), `sync_binlog=1` +
  `innodb_flush_log_at_trx_commit=1` (we SIGKILL the process, its disk
  survives; losses are *replication* losses, the measurement stays honest).
- **Two replicas**, so candidate selection is a real decision.
- **Router = `router.json`**, atomically rewritten at cutover. No proxy, on
  purpose — the lab stays transparent; the production proxy goes in §7.
- Node names are their role *at boot*. After a failover the names lie
  (`primary` may be a replica of `replica1`) — exactly like the hostnames in a
  real incident. `router.json` is the only truth.

### The lag injection (read this before crying "rigged")

On one laptop, replication lag is microseconds and an async crash would lose
~nothing — flaky theater. Real async crashes lose data because replicas run
seconds behind. So `kill_primary.py` first injects a deterministic 2s
partition — `STOP REPLICA IO_THREAD` on both replicas — then SIGKILLs the
primary mid-partition. Both async and semisync drills get the **identical**
partition + kill; only the replication mode differs. Async acks through the
partition and loses the window; semi-sync blocks commits (no acks → nothing to
lose). Same crash, honest comparison.

Amusing war story: the first implementation SIGSTOPed the replica containers
instead. Zero loss, every time. The primary's dump thread kept pushing binlog
into the frozen replicas' kernel **socket buffers**, and after SIGCONT they
drained events from an already-dead primary. The kernel does not care about
your drill.

### The promotion algorithm (`failover.py`, ~150 lines)

What orchestrator/MHA do, readable:

1. Confirm the primary is dead (never promote against a live one).
2. Freeze survivors' IO threads; snapshot each one's received ∪ executed set.
3. Candidate = maximal set by **containment** (if two survivors each hold
   GTIDs the other lacks → abort loudly; no safe automatic answer exists).
4. Drain: `WAIT_FOR_EXECUTED_GTID_SET(retrieved)` — apply everything received.
5. Tripwire: no survivor may hold GTIDs the candidate lacks (errant-to-be).
6. Promote: `STOP REPLICA; RESET REPLICA ALL; SET PERSIST super_read_only=OFF`.
7. Repoint other survivors: `CHANGE REPLICATION SOURCE ... AUTO_POSITION=1`.
8. Atomically flip `router.json`.

The printed timeline is your RTO, decomposed:

```
kill_ts            T  +0.000s
detect_ts          T  +0.384s
candidate_ts       T  +0.403s
relay_drained_ts   T  +0.406s
promoted_ts        T  +0.445s
router_flip_ts     T  +0.520s
RTO (kill -> router flip)      0.520s
```

(Half a second because detection here is one failed TCP connect. Production
detection — quorum checks, flap suppression — is where real RTO goes; see §7.)

## 5. Runbook

```bash
make demo          # the whole story: async loss vs semisync zero, one table
```

Or step by step:

```bash
make up install bootstrap seed     # 3 nodes, GTID replication, 10k rows
make drill-async                   # traffic → partition+kill → failover → verify
make restore                       # clone-rebuild the dead node, rejoin
make drill-semisync                # same crash, semisync on
make drill-degrade                 # the silent-degradation trap
make drill-zombie                  # split-brain → detect → fence → rebuild
make clean                         # pristine machine
```

### Drill 1 — `make drill-async`: async loses acked commits

Expected output (numbers vary by ±20%):

```
acked by primary            :    768 rows
acked but LOST after failover:   172 rows   <-- RPO
    lost seq range: 10289 .. 10479
RTO (kill -> router flip)      0.458s
```

172 rows the application was told were durable. Note the lost range is
*contiguous and ends at the crash*: the acked tail that never left the
primary.

### Drill 2 — `make drill-semisync`: same crash, zero loss

```
acked by primary            :    454 rows
acked but LOST after failover:     0 rows   <-- RPO
writer commit latency        : avg 11.97ms, p95 39.04ms
```

Fewer acked rows in the same wall time (commits blocked during the partition —
that's the mechanism working) and a fatter latency tail. That's the price tag.

### Drill 3 — `make drill-degrade`: semi-sync silently betrays you

The partition lasts 6s, longer than the 3s `rpl_semi_sync_source_timeout`:

```
WARNING primary Rpl_semi_sync_source_status flipped ON -> OFF
        (semisync has SILENTLY degraded to async)
...
acked but LOST after failover:   254 rows   <-- RPO
```

Semi-sync was "enabled" the whole time. After the timeout the primary stopped
waiting, resumed acking, and the crash took the acked tail — *more* than the
plain async drill, because degraded mode had 3 extra seconds of backlog. The
lesson is operational, not configurational: **alert on
`Rpl_semi_sync_source_status = OFF`, or you don't have semi-sync — you have a
morale boost.**

### Drill 4 — `make drill-zombie`: split-brain, detection, fencing, rebuild

```
replica2 is back. read_only=0   <-- an UNFENCED zombie, happy to take writes
...
SPLIT-BRAIN DETECTED
errant GTIDs on replica2: 3285ba76-...:1257-1490
divergent rows on replica2: 50
...
AFTER FENCE + REBUILD
errant GTIDs on replica2: (none)
row counts: replica2=10601  replica1=10601  match=True
```

The zombie's divergent rows are **not merged** — they're wiped by the rebuild.
That's the honest call: rows that exist only on a fenced ex-primary are
evidence for the incident report (verify.py printed them), not data to quietly
resurrect next to a lineage that never saw them. If those rows were customer
money, the remediation is an application-level replay from the journal — a
human decision, not a replication feature.

### Drill 5 (manual) — promote the laggard on purpose

```bash
make traffic-start
docker exec lab04-replica2 mysql -plab -e "STOP REPLICA SQL_THREAD"  # lag it
sleep 5
make kill-primary
.venv/bin/python scripts/failover.py --force-candidate replica2      # wrong on purpose
```

Watch the tripwire fire: `!! replica1 holds GTIDs the new primary will NOT
have` — you just chose to lose data that had already survived. This is why
candidate selection is step 3, not a coin flip.

### Why rebuilds use CLONE, not binlog replay

A tempting rebuild is "wipe GTID state, auto-position from zero, replay
everything from the new primary's binlog." It **cannot work for an
ex-primary**: replicas silently discard binlog events stamped with their own
`server_id` (the circular-replication guard), so the node skips every
transaction it originally authored, then dies applying rows to tables whose
`CREATE` it just skipped. Rebuilds come from *snapshots*: `rebuild.py` uses
the CLONE plugin — physical InnoDB page transfer from a donor, `gtid_executed`
included — then normal replication for the delta. Same recipe at 500M rows,
just a longer copy. (Container quirk: the recipient shuts down post-clone
because mysqld is PID 1 with no supervisor; the script docker-starts it.)

## 6. What the journal teaches about ambiguity

The writer distinguishes three outcomes per commit: **acked** (MySQL said
durable), **lost** (acked, then gone — the RPO), and **ambiguous** (connection
died mid-commit; no answer either way). Ambiguous is not loss — it's
*unknowable at commit time*, the two-generals problem in one row. The lab
resolves it the way production code should: retry the same `seq` after
reconnect; the `UNIQUE(seq)` key answers the question. Duplicate-key error →
it landed, count it acked. Success → it hadn't. **An idempotency key turns
"ambiguous" into an answerable question.** Design your writes so retries are
safe, or failovers will double-charge someone.

## 7. Production checklist — laptop vs. pager

- **Don't hand-roll failover.** This lab's `failover.py` is for understanding.
  Run [orchestrator](https://github.com/openark/orchestrator), MHA, or
  MySQL InnoDB Cluster / Group Replication (which replaces the whole
  promote-and-repoint dance with a Paxos-family group membership).
- **Router**: `router.json` becomes ProxySQL / HAProxy / MySQL Router / a VIP,
  with connection draining — killing established connections to the old
  primary is part of fencing, not a nicety.
- **Detection is the hard 90% of RTO.** One failed connect ≠ dead. Quorum
  probes from multiple vantage points, flap damping, and a decision timeout
  you've actually rehearsed. Auto-failover without fencing is how you get the
  zombie drill at 3am, at scale, with lawyers.
- **Semi-sync hygiene**: alert on `Rpl_semi_sync_source_status=OFF` and on
  `Rpl_semi_sync_source_no_tx` increasing; with 2+ replicas set
  `rpl_semi_sync_source_wait_for_replica_count` deliberately; know your
  timeout and what degradation means for your RPO promise.
- **Errant-GTID check before every promotion** — orchestrator does this;
  if you're manual, `GTID_SUBTRACT` every survivor against the candidate.
- **Rebuilds at scale**: CLONE plugin (as here) or restore-from-backup + delta.
  Never "it'll catch up eventually" a diverged node back in.
- **Managed cloud** ≠ exempt: RDS Multi-AZ promises RPO≈0 via synchronous
  block-level replication (different mechanism, same AFTER_SYNC idea); classic
  read-replica promotion is async and CAN lose acked writes; Aurora moves the
  problem into a shared storage layer. Read the actual durability contract,
  then design the drill that verifies it.
- **Run this drill in staging quarterly.** RPO/RTO numbers you haven't
  measured since the topology changed are fiction.

## 8. Interview questions to answer without notes

1. Semi-sync is enabled — can you still lose acked writes? (Three ways:
   degradation timeout, AFTER_COMMIT wait point, more failed replicas than
   your ack count.)
2. AFTER_SYNC vs AFTER_COMMIT — where exactly is the ack in each, and what
   does the difference mean for *other sessions* reading the primary?
3. How do you pick the promotion candidate, and why compare
   `Retrieved_Gtid_Set` instead of `gtid_executed`?
4. What's an errant transaction? How do you detect one, and what are the two
   ways this lab manufactures them?
5. Why can't an ex-primary be rebuilt by replaying the new primary's binlog
   from GTID zero?
6. The writer got a connection error during COMMIT. Was the transaction
   durable? What schema feature makes that question answerable?
7. What's the RPO and RTO of this lab's design in each mode, and which
   component dominates RTO in production?
8. Why is `super_read_only` + PERSIST the right fencing default for replicas,
   and why is the old primary specifically dangerous after a crash?

## 9. File map

```
docker-compose.yml     3 nodes; GTID, semisync+clone plugins, durability flags
sql/schema.sql         events(id, seq UNIQUE, payload, created_at)
scripts/common.py      node map, router, GTID algebra helpers
scripts/bootstrap.py   RESET MASTER everywhere + wire replication (idempotent)
scripts/seed.py        10k rows
scripts/traffic.py     the journaling writer (acked/ambiguous, retry-by-seq)
scripts/kill_primary.py  partition injection + SIGKILL (+ semisync watcher)
scripts/failover.py    the promotion algorithm
scripts/verify.py      journal-vs-survivor diff: RPO, RTO timeline, GTID snapshot
scripts/semisync.py    enable|disable|status (AFTER_SYNC, 3s timeout)
scripts/fence.py       super_read_only + errant-GTID report
scripts/rebuild.py     CLONE-based rebuild + rejoin (also `make restore`)
scripts/drill.py       orchestrates drills 1–4
scripts/report.py      the async-vs-semisync comparison table
```
