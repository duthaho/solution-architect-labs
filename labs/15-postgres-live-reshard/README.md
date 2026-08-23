# Lab 15 — Reshard a live Postgres, the way the pros stopped double-writing

One overloaded Postgres, 500k rows, continuous writes — split it onto two
shards with a sub-second pause, prove not one acked write was lost, and keep
a rollback that still works *after* the cutover. This is the approach Figma
chose because they explicitly **rejected double-writes** ("challenging to
implement without taking downtime or compromising on consistency") and the
one Notion used to triple their fleet: **logical replication moves the data,
an LSN gate decides the moment, and a reverse replication stream keeps the
exit open**.

Lab 06 hand-rolled the other school — application double-writes, chunked
backfill, shadow reads — on MySQL. This lab is the counter-argument: the
database already knows how to copy itself; your job shrinks to routing,
gating, and verification. Both schools ship real migrations. Knowing *why*
each exists is the actual lesson.

## The problem

The monolith is the source of truth and it is running out of headroom. Every
row belongs to exactly one workspace (`workspace_id` — Notion's shard key,
chosen because users query within one workspace), so the data model shards
cleanly. The hard part is never the split — it's the four ways the move goes
wrong under traffic:

1. **The flip happens while replication is behind.** Whatever the old
   primary acked in that lag window doesn't exist on the new side. The
   naive drill makes this deterministic: ~400 acked writes, gone or stale.
2. **Logical replication doesn't carry everything.** Sequences, DDL, and
   large objects stay behind ([PG docs — restrictions][pgr]). The shard
   that "has all the data" will happily mint id 1 twice — once as a loud
   duplicate-key error, once as a **silent global duplicate** on the shard
   that doesn't own id 1.
3. **Rollback dies at cutover unless you build its transport in advance.**
   The moment writes land on the shards, the old primary is stale. Figma
   and Notion both kept a **reverse replication stream** (new → old) armed
   from the instant of cutover, so rolling back never means losing
   post-cutover writes.
4. **Verification by vibes.** "Row counts match" is not a proof. Notion ran
   two independent verifiers written by different people; this lab's
   `verify.py` recomputes checksums, boundaries, uniqueness, and replays
   every acked write from the journal — and `VERIFY_INVERT=1` proves the
   gate isn't vacuous by making it certify the naive drill's damage.

## Architecture

```
                       app-side router (router_state.json, atomic rename)
                          writes_gated · authoritative · sequences_fixed
                                          │
             ┌─── before cutover ─────────┼───────── after cutover ───┐
             ▼                            │                           ▼
        ┌─────────┐   pub_shard0 WHERE (workspace_id % 2 = 0)   ┌─────────┐
        │  mono   │ ──────────────────────────────────────────▶ │ shard0  │
        │ 500k    │   pub_shard1 WHERE (workspace_id % 2 = 1)   ├─────────┤
        │ rows    │ ──────────────────────────────────────────▶ │ shard1  │
        └─────────┘                                             └─────────┘
             ▲                 pub_back0 / pub_back1 (unfiltered)     │
             └────────────────────────────────────────────────────────┘
                  reverse stream — created INSIDE the cutover gate,
                  so no post-cutover write can predate its slot

        journal.jsonl — every write the app ever acked, replayed by verify.py
```

The router is deliberately primitive — a JSON file flipped by `rename()` —
because that is the honest laptop version of what Notion ran (application
routing as the single source of truth) and what Figma built DBProxy to do.

## Deep dives

### Why not double-writes (and why lab 06 does them anyway)

Double-writes give you a second copy at the cost of owning **every** failure
mode yourself: partial writes, ordering, backfill vs live-write races,
repair queues. Lab 06 builds all of that to make it visible. Figma's team
looked at the same list and chose Postgres's own WAL instead: logical
replication ports a subset of tables, works across major versions, and —
decisive point — supports **reverse replication for rollback**. Slack's
Vitess migration is the counterexample that double-writes *at scale* work
too (backfill + app double-writes + a double-read diffing system) — with a
team maintaining Vitess to pay for it.

### The LSN gate, and which signal actually works

The cutover moment is defined by one comparison: the source's
`pg_current_wal_lsn()` captured **after** writes quiesce, versus what the
replicas have replayed. This lab's spike found the trap: the subscriber-side
`pg_replication_origin_status.remote_lsn` only advances when logical changes
commit — after a quiesce, with only checkpoint records in the WAL, it stalls
*below* the captured LSN forever. The publisher-side
`pg_stat_replication.replay_lsn` is fed back by the apply worker on
keepalives and passes the gate. Gate on the publisher's view
([PG docs — monitoring][pgm]); it is also the one place that sees all
subscribers at once.

One honesty note on the quiesce itself: this lab drains in-flight writes by
watching the ack journal go quiet — observation, not a hard barrier. A
client that routed just before the gate and stalls long enough could still
commit late; the audit would *catch* that loss loudly rather than miss it,
but preventing it outright needs the production-grade stop: PgBouncer
`PAUSE` plus `REVOKE` on the old primary, which is exactly what Figma ran
(and this lab parks as a variant).

### The replica-identity landmine

A row-filtered publication that publishes `UPDATE`/`DELETE` requires the
filter columns to be covered by the table's **replica identity** — and if
they aren't, the failure is not degraded replication but `cannot update
table "docs"` on **every application update** the moment the publication
exists. The fix is in the schema before any pub/sub is created: a unique
index on `(workspace_id, id)` and `REPLICA IDENTITY USING INDEX`, on every
node — the subscriber must also send those columns once it becomes a
publisher for the reverse stream. Set it late and the poison is permanent:
identity changes aren't retroactive, and one pre-change record wedges the
apply worker in a 5-second retry loop forever.

### Sequences: the failure that survives a perfect copy

`copy_data` moves rows, not sequence state — the shard's `docs_id_seq` still
answers `1` while the shard holds 250k rows ([PG docs — restrictions][pgr]).
The drill shows the full ladder: the loud duplicate-key on the shard that
owns id 1; the **silent global duplicate** on the shard that doesn't (the
worse failure — it corrupts, then poisons the reverse stream later); the
tempting `setval(max(id))` that still lets both shards race up the same
range; and the fix that holds at N=2 — interleaved sequences (`INCREMENT BY
2`, disjoint parity). The production answer at arbitrary N is lab 13's
Snowflake ids. And the trap mirrors on the way back: after a rollback, the
old primary's sequence has never heard of the ids the shards minted —
re-syncing it is a rollback step, not an afterthought.

### The reverse stream is armed inside the gate — this is not optional

Two orderings fail. Create the reverse stream *after* writes resume, and
every write in the gap predates its replication slot — unrecoverable on
rollback. Leave the forward subscriptions alive while the reverse stream
runs, and every reverse-applied row gets decoded forward again into a
duplicate-key conflict that wedges the subscription. The only safe sequence
is Notion's "flip the replication streams" — inside the pause: quiesce →
LSN gate → drop forward subs → create reverse pubs/subs (`copy_data =
false`) → flip routing → resume. This lab's pause, all included: **~0.5–0.8
s** (Figma: ~30 s partial availability on the same maneuver at fleet scale;
Notion: "about a second of a saving spinner").

### What a real verifier owes you

Every check recomputes from scratch: shard-union row counts against mono,
per-range `md5(string_agg(... ORDER BY id))` checksums of each partition
**as a union** (never per-shard against whole-mono), filter-boundary
ownership, cross-shard PK uniqueness, and a journal replay of every acked
write against whoever is authoritative *now* — the gate reads
`router_state.json` and the actual pub/sub topology to know which phase's
invariants it owes. Comparisons run under a brief write gate with an LSN
wait first — the same pause-before-dark-read Notion used.

## Runbook

```bash
make demo        # the whole story, end to end (~2 min)

# or step by step:
make up install bootstrap seed     # 3× postgres:17, schema, 500k rows
make bench-index                   # the Notion/Figma index trick, measured
make replicate                     # filtered pubs, subs, initial sync (~7 s)
make traffic-start                 # journaled writes against the router
make replicate-status              # lag per shard, row counts
make drill-naive                   # the disaster: flip mid-lag, count losses
make verify-naive                  # inverted gate certifies the damage
make reset-shards                  # baseline again: truncate, resync
make cutover                       # quiesce → LSN gate → stream flip → route
make verify                        # post-cutover invariants
make drill-sequence                # the id traps, then interleaved fix
make drill-rollback                # live writes on shards, then roll back
make verify                        # every acked write accounted for on mono
make traffic-stop && make clean    # pristine machine
```

Measured on this lab's seed (your numbers will vary, the shape won't):

| stage | result |
|---|---|
| initial sync, 250k rows, indexes kept | ~6.0 s |
| initial sync, indexes dropped + rebuilt | ~3.7 s + 0.8 s |
| naive cutover, 3 s lag window | ~400 acked writes missing/stale |
| gated cutover write pause | ~0.5–0.8 s, 0 lost |
| rollback write pause (after cutover) | ~0.4 s, 0 lost, inserts included |

## Production checklist (500M rows and a pager)

- **Shard count:** pick many logical shards over few physical ones (Notion:
  480 logical / 32 physical) so the next split is a move, not a re-split.
- **Initial sync:** drop destination indexes first; days become hours
  (Notion: ~3 d → ~12 h). Watch `pg_stat_subscription_stats` for sync
  worker errors; size `max_logical_replication_workers` for table count.
- **Slots retain WAL.** An orphaned slot fills the publisher's disk. Alert
  on `pg_replication_slots.wal_status` and slot age. Dropping a
  subscription with `slot_name = NONE` orphans the remote slot — clean it.
- **Long transactions stall catch-up** — decoding ships only committed
  work. Kill or wait out long writers before the window, or the LSN gate
  never closes.
- **The gate needs a real quiesce**: PgBouncer `PAUSE` + `REVOKE` on the
  old primary (Figma canceled the ~10 stragglers in-flight). An app-side
  flag only gates apps that honor it.
- **Sequence/id strategy decided before cutover**, not in the incident:
  interleaved ranges at small N, coordinated id service (lab 13) beyond.
- **Rehearse the rollback under load** until it's boring, and keep the
  reverse stream until the migration is declared done — Notion kept old
  shards consuming the new fleet's WAL for the whole bake period.
- **Two verifiers, two authors** (Notion): a checksum/range verifier and a
  dark-read sampler with a replication wait; require ~100% equivalence
  before the flip, keep sampling after.
- **Connection math**: doubled topology ≈ doubled connections during the
  window — Notion had to re-plan PgBouncer clusters for exactly this.

## Interview questions

1. Figma rejected double-writes and Slack built their migration on them.
   Give the strongest argument for each choice.
2. The cutover gate compares a captured LSN against replica progress. Why
   must the LSN be captured *after* writes quiesce, and why gate on the
   publisher's `replay_lsn` rather than the subscriber's origin LSN?
3. Your row-filtered publication filters on a column outside the replica
   identity. What breaks, and when — replication, or something worse?
4. A migration "completes" and inserts start failing with duplicate keys on
   one shard but not the other. Reconstruct what happened.
5. Why must the reverse replication stream be created while writes are
   still paused? Name both failure modes of doing it late or leaving the
   forward stream up.
6. After rolling back, the old primary starts minting ids that collide.
   Why, and what does that tell you about rollback runbooks in general?
7. Your verifier compares each shard's row count to the monolith's. What
   two classes of corruption does that comparison miss?

## File map

```
docker-compose.yml     lab15-mono + lab15-shard0/1, postgres:17, wal_level=logical
sql/schema.sql         docs table; the replica-identity index the whole lab rests on
scripts/common.py      connections, shard math, seeded RNG, journal + router-state IO
scripts/bootstrap.py   schema to all nodes, replication cleared, artifacts reset
scripts/seed.py        500k deterministic rows on mono
scripts/router.py      the app-side shard map: gate-aware, atomic flips
scripts/traffic.py     journaled write traffic; insert path freezes until sequences fixed
scripts/replicate.py   filtered pubs/subs, initial sync, lag status, LSN gate, reset
scripts/drill_naive.py flip mid-lag, decommission, count the acked-write damage
scripts/cutover.py     quiesce → LSN gate → flip replication streams → flip routing
scripts/drill_sequence.py  loud dup, silent global dup, setval trap, interleaved fix
scripts/drill_rollback.py  live shard writes, reverse-stream rollback, audit on mono
scripts/verify.py      phase-aware invariant gate + VERIFY_INVERT naive-damage proof
scripts/bench_index.py initial sync: indexes kept vs dropped + rebuilt
```

Sources: [Notion — Sharding Postgres][n1] · [Notion — The Great
Re-shard][n2] · [Figma — How Figma's databases team lived to tell the
scale][f1] · [Figma — Growing pains][f2] · [Slack — Scaling datastores with
Vitess][s1] · [PostgreSQL docs — logical replication restrictions][pgr] ·
[monitoring][pgm]

[n1]: https://www.notion.com/blog/sharding-postgres-at-notion
[n2]: https://www.notion.com/blog/the-great-re-shard
[f1]: https://www.figma.com/blog/how-figmas-databases-team-lived-to-tell-the-scale/
[f2]: https://www.figma.com/blog/how-figma-scaled-to-multiple-databases/
[s1]: https://slack.engineering/scaling-datastores-at-slack-with-vitess/
[pgr]: https://www.postgresql.org/docs/current/logical-replication-restrictions.html
[pgm]: https://www.postgresql.org/docs/current/logical-replication-monitoring.html
