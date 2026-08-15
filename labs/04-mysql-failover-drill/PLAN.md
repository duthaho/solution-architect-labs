# Lab 04 — MySQL failover drill: measure the data-loss window

> **Status: PLANNED — nothing implemented yet.**
> This file is the full build plan. To continue in a fresh session, read this file
> top-to-bottom, then start at the first unchecked milestone in [Milestones](#milestones).

## The pitch

Labs 01–03 are about *moving* data safely. This lab is about *losing* it — on purpose,
in a controlled way, so you know exactly what happens before the pager teaches you.

`make demo` literally `docker kill`s the primary while a writer app is committing rows,
promotes a replica, and prints:

```
acked by primary but LOST after failover: 37 rows
```

Then it repeats the exact same crash with lossless semi-sync replication enabled and prints `0`.

Core lessons:

1. **Async replication loses acked commits.** Quantify the window, don't hand-wave it.
2. **Semi-sync (AFTER_SYNC) closes the window** — at a latency cost, and it silently
   degrades back to async on timeout (a drill reproduces this trap).
3. **Promotion is an algorithm, not a button**: pick the most-caught-up replica by GTID,
   wait for relay-log apply, repoint the other replica, flip the router.
4. **The old primary is a zombie until fenced.** Reproduce split-brain, detect errant
   GTIDs, remediate.

## Architecture

```
                         ┌────────────────────────────┐
 traffic.py (writer) ────► router.json (current primary)
   │  journals every        └────────────────────────────┘
   │  ACKED commit to               │ read on each (re)connect
   ▼  journal.jsonl                 ▼
┌─────────────┐  async/semisync  ┌─────────────┐
│ mysql-primary│ ───────────────► │ mysql-replica1│   (candidate A)
│  :3307       │ ───────────────► │ mysql-replica2│   (candidate B, lagged sometimes)
└─────────────┘                  └─────────────┘  :3308 / :3309
```

- **3× MySQL 8.0 containers** (`mysql-primary`, `mysql-replica1`, `mysql-replica2`).
  Two replicas so candidate selection is a real decision, not a formality.
- **GTID everywhere**: `gtid_mode=ON`, `enforce_gtid_consistency=ON`,
  `SOURCE_AUTO_POSITION=1`. GTID sets are how we compare "what the primary acked" vs
  "what each replica actually has".
- **Replicas** run `super_read_only=ON` from boot (this *is* fencing done right).
- **Router = `router.json`** (host/port of current primary). The writer re-reads it on
  every reconnect. Cutover = atomically rewrite this file. (No HAProxy — keeps the lab
  transparent; production equivalent goes in the README's checklist.)
- **Writer (`traffic.py`)**: single-threaded loop inserting monotonically increasing
  `seq` values; appends `{seq, gtid?, ts}` to `journal.jsonl` **only after the commit is
  acked**. On connection error: retry loop via router. Journal = ground truth of "the
  application was told this write is durable".
- **Loss measurement (`verify.py`)**: after failover, `SELECT seq FROM events` on the new
  primary, diff against acked journal. Report: acked-and-present, **acked-but-LOST**,
  in-flight-unacked (sent, no ack — ambiguous, reported separately). Also snapshot
  `gtid_executed` of every surviving node for the post-mortem printout.
- Ports 3307/3308/3309 to avoid colliding with labs 02/03.

## File tree (target)

```
labs/04-mysql-failover-drill/
├── README.md               # deep-dive (written last, see outline below)
├── PLAN.md                 # this file (delete or trim when lab is Ready)
├── docker-compose.yml      # 3 mysql services, healthchecks, config via command: flags
├── Makefile
├── requirements.txt        # PyMySQL pinned (match lab 02/03 versions)
├── sql/schema.sql          # events(id PK auto_inc, seq BIGINT UNIQUE, payload, created_at)
└── scripts/
    ├── common.py           # conns via router.json, GTID helpers (gtid_executed, subtract)
    ├── bootstrap.py        # wire replication: replicas -> primary, AUTO_POSITION, verify
    ├── seed.py             # small seed (10k rows) — volume isn't the point here
    ├── traffic.py          # journaling writer with reconnect-via-router loop
    ├── semisync.py         # enable|disable|status  (rpl_semi_sync_{source,replica}, AFTER_SYNC)
    ├── kill_primary.py     # docker kill -s KILL mysql-primary (+ record kill timestamp)
    ├── failover.py         # the promotion algorithm (see below)
    ├── verify.py           # journal-vs-new-primary diff + GTID report
    ├── zombie.py           # restart old primary UN-fenced + point a "stale" writer at it
    ├── fence.py            # super_read_only=ON old primary, detect errant GTIDs
    └── rebuild.py          # rejoin old primary as replica of new primary (GTID auto-pos)
```

## The promotion algorithm (`failover.py`)

This is the heart of the lab — mirror what orchestrator/MHA do, in ~150 readable lines:

1. Confirm primary is dead (connect attempt with short timeout).
2. Poll both replicas: `SELECT @@gtid_executed`, `Retrieved_Gtid_Set` from replica status.
3. **Candidate = replica with the largest retrieved GTID set** (most caught-up on
   *received* binlog, not just applied).
4. Wait for candidate to **apply** everything it retrieved
   (`WAIT_FOR_EXECUTED_GTID_SET(retrieved_set)` — draining the relay log).
5. Optional best-effort: if the other replica retrieved GTIDs the candidate lacks, error
   loudly (in this lab's topology it can happen; discuss why in README).
6. Promote: `STOP REPLICA; RESET REPLICA ALL; SET GLOBAL super_read_only=OFF, read_only=OFF`.
7. Repoint the other replica: `CHANGE REPLICATION SOURCE TO` new primary, AUTO_POSITION.
8. Flip `router.json` (write temp + atomic rename).
9. Print a timeline: kill ts → detection ts → promotion ts → router-flip ts = **RTO**;
   verify.py's lost-row count = **RPO, measured not estimated**.

## Makefile targets

Follow lab 02's conventions (`.venv`, `up/down/clean/install`, `traffic-start/stop`,
pid+log files, `demo`, `clean` removes volumes + journal + router.json):

```
up / down / clean / install / bootstrap / seed
traffic-start / traffic-stop
semisync-on / semisync-off
kill-primary        # SIGKILL the current primary container
failover            # run promotion algorithm
verify              # loss report (RPO) + timeline (RTO)
drill-async         # full: traffic → kill → failover → verify   (expect loss > 0)
drill-semisync      # same with semi-sync on                     (expect loss = 0)
drill-degrade       # SIGSTOP both replicas → semisync times out → kill → loss AGAIN
drill-zombie        # restart old primary unfenced → split-brain → detect → fence → rebuild
demo                # drill-async + drill-semisync + comparison table
```

Note: `kill-primary`/`zombie` scripts shell out to `docker` — the lab manipulates its own
containers by fixed container names (set `container_name:` in compose).

## Failure drills (README section, each with expected output)

| # | Drill | What it proves |
|---|-------|----------------|
| 1 | `drill-async` | Async replication loses acked commits; loss is measurable, nonzero |
| 2 | `drill-semisync` | Lossless semi-sync (AFTER_SYNC): same crash, 0 acked rows lost |
| 3 | `drill-degrade` | Semi-sync **silently falls back to async** after `rpl_semi_sync_source_timeout` (SIGSTOP both replicas, watch `Rpl_semi_sync_source_status` flip OFF, then kill → loss despite "semi-sync enabled"). The monitoring lesson. |
| 4 | `drill-zombie` | Un-fenced old primary boots, a writer with a stale router keeps committing to it → divergence. Detect errant GTIDs (`GTID_SUBTRACT(zombie, new_primary)`), fence, decide the fate of divergent rows, `rebuild.py` rejoins it as replica. |
| 5 | manual: promote the *lagging* replica on purpose (failover.py `--force-candidate replica2` after `STOP REPLICA` on it for a while) | Why candidate selection matters; the errant-GTID mess when the better replica has more data than the new primary |

## Key MySQL config decisions (pin these, they bite)

- Image: `mysql:8.0` (8.0.26+ so semisync uses `rpl_semi_sync_source_*` naming; install
  plugins `rpl_semi_sync_source` on primary-capable nodes and `rpl_semi_sync_replica` on
  all — every node gets both since roles swap during the lab).
- `rpl_semi_sync_source_wait_point = AFTER_SYNC` (lossless; contrast with AFTER_COMMIT
  in README — phantom-read window).
- `rpl_semi_sync_source_timeout`: set LOW for drill 3 (e.g. 3000ms) so degradation
  reproduces in seconds.
- `sync_binlog=1`, `innodb_flush_log_at_trx_commit=1` on all nodes (don't let fsync
  laziness pollute the loss measurement — we want loss caused by *replication*, not by
  the crashed node's own durability; primary is SIGKILLed, not host-crashed, so its own
  disk state survives — which is exactly what makes zombie divergence interesting).
- Healthchecks on all three; `--wait` in `make up`.

## README.md outline (write last)

1. The problem: "the primary is gone" is not an edge case, it's a scheduled event.
2. Why the naive approach fails (promote whoever, repoint, hope): lost acked writes,
   wrong candidate, zombie primary, split-brain.
3. Concepts: async vs semi-sync (AFTER_SYNC vs AFTER_COMMIT), GTID mechanics,
   retrieved vs executed sets, errant transactions, fencing/STONITH, RPO vs RTO.
4. Runbook: happy path + each drill, exact commands + expected output blocks.
5. Production checklist: orchestrator/MHA/InnoDB Cluster (group replication) vs DIY;
   proxy layer (ProxySQL/HAProxy) instead of router.json; connection draining;
   monitoring `Rpl_semi_sync_source_status` (the drill-3 trap); semisync with 2+ replicas
   (`rpl_semi_sync_source_wait_for_replica_count`); what RDS/Aurora actually promise;
   CLONE plugin for rebuilds at scale; never auto-failover without fencing.
6. Interview questions (no notes): e.g. "semi-sync is on — can you still lose acked
   writes?" (yes: drill 3), "what's an errant transaction?", "AFTER_SYNC vs
   AFTER_COMMIT?", "how do you pick the promotion candidate?", "RPO/RTO of this design?".

## Milestones

Work top-to-bottom; each milestone ends green and demoable. Check boxes as you go.

- [ ] **M1 — Infra**: compose file (3 nodes, GTID, healthchecks, fixed container names,
      ports 3307–3309), `common.py`, `bootstrap.py` (replication wired + verified:
      insert on primary appears on both replicas), `sql/schema.sql`, Makefile
      `up/install/bootstrap/clean`. Gate: `make up bootstrap` idempotent from clean.
- [ ] **M2 — Writer + loss measurement**: `seed.py`, `traffic.py` (journaling +
      reconnect-via-router), `verify.py`. Gate: with traffic running, manually kill
      primary, manually promote replica1 via mysql client, flip router.json by hand,
      `make verify` prints a correct nonzero loss report.
- [ ] **M3 — Automated failover**: `failover.py` (full algorithm incl. candidate
      selection + relay drain + repoint + router flip + RTO timeline), `kill_primary.py`.
      Gate: `make drill-async` end-to-end, loss > 0, RTO printed.
- [ ] **M4 — Semi-sync**: `semisync.py`, drill target. Gate: `make drill-semisync`
      → 0 acked rows lost; README notes measured latency delta on the writer.
- [ ] **M5 — Advanced drills**: `drill-degrade` (SIGSTOP replicas, status flip observed
      in output), `zombie.py` + `fence.py` + `rebuild.py` for `drill-zombie`
      (divergence detected via GTID_SUBTRACT, zombie rejoined). Gate: both drills
      scripted with expected-output blocks.
- [ ] **M6 — Polish**: `make demo` (async vs semisync comparison table), full README
      per outline, root `README.md` row 04 flipped to ✅ Ready, `make clean` verified
      pristine, PLAN.md trimmed to a short "design notes" appendix or deleted.

## Related labs

The other ideas from the 2026-08-15 ideation session are now planned labs with their
own `PLAN.md`: 05 (fencing tokens — the natural sequel to this lab, shared split-brain
theme), 06 (live resharding), 07 (cache consistency), 08 (idempotent event processing,
absorbing poison pills/DLQ), 09 (expand/contract deploys, the series capstone). See the
root `README.md` table.
