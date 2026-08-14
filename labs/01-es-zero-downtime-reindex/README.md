# Lab 01 — Elasticsearch Zero-Downtime Reindex of a Large Live Index

> Change the mapping of a multi-million-document index that is receiving
> continuous writes, with **zero read downtime**, a **seconds-long write pause**,
> and **provably zero data loss** — then verify it, then roll it back.

## 1. The problem

Elasticsearch mappings are largely immutable. You cannot change a field's type,
its analyzer, or its `scaling_factor` in place. The day search needs
autocomplete, or aggregations need `sku` as `keyword` instead of `text`, you
must build a **new index** and move every document into it.

On a laptop with a static dataset that's one `_reindex` call. In production it
is hard because of one fact: **the data keeps changing while you copy it.**

Naive approaches and how they fail:

| Approach | Failure mode |
|---|---|
| Stop the app, reindex, start the app | Hours of downtime on a big index. Career-limiting. |
| `_reindex` while live, then switch | Every write that happened during the copy is lost or stale. Silent data gap — the worst kind. |
| Point the app at the new index first, then copy | Reads miss all old documents until the copy finishes. |
| Dual-write from day one "just in case" | Solves this migration but you still need a backfill, and now every write path carries permanent complexity. |

This lab builds the real solution and — crucially — **proves** it lost nothing.

## 2. Architecture

The load-bearing decision was made before the migration was ever needed:

```
            ┌───────────────────────────┐
   reads ──►│  alias: products-read     │──┐
            └───────────────────────────┘  │      ┌──────────────┐
                                           ├─────►│ products_v1  │
            ┌───────────────────────────┐  │      └──────────────┘
  writes ──►│  alias: products-write    │──┘
            └───────────────────────────┘
```

**The application never knows a concrete index name.** Aliases are the unit of
indirection, and Elasticsearch swaps a set of alias changes **atomically** in a
single `_aliases` call. No client ever observes an in-between state. This is
the ES equivalent of `RENAME TABLE` cutover in MySQL migrations — same idea,
same reason.

If your current production index doesn't sit behind an alias: that is
migration zero. (You can add an alias to an existing index with zero impact,
then deploy the app change to use it.)

### Migration data flow

```
 phase                v1 (source)                 v2 (target)
 ─────────────────────────────────────────────────────────────────────
 baseline copy        ◄── live writes             ◄── _reindex slices=auto
 catch-up loop        ◄── live writes             ◄── delta: updated_at >= cursor
 cutover (~1-2s)      writes BLOCKED (retrying)   ◄── final delta pass
                      ── atomic alias swap ──────►
 after                kept for rollback           ◄── live writes
```

## 3. Deep dive: the five hard sub-problems

### 3.1 Copying millions of docs without melting the cluster

`_reindex` is a scroll + bulk loop running inside the cluster. The lab uses:

- **`slices=auto`** — one slice per shard, copying in parallel.
- **`wait_for_completion=false` + Tasks API** — a big reindex outlives any HTTP
  timeout; you get a task id and poll it. In production you also survive your
  own laptop disconnecting.
- **`requests_per_second`** (exposed as `--rps`) — the throttle you *will* need
  in production so the copy doesn't starve live search traffic. Throttling
  works by injecting waits between scroll batches.
- Target index tuned for load: **`refresh_interval=-1`**, **`replicas=0`**.
  No per-second segment refresh, no replication write amplification. Both are
  restored after cutover. (Replicas are then built by segment copy, which is
  cheaper than indexing every doc twice.)

### 3.2 Catching up with writes that happened during the copy

Every document carries `updated_at` (epoch millis), written by the app on every
write. The orchestrator:

1. Records `cursor = now - overlap` **before** starting the baseline copy.
2. After the baseline, repeatedly reindexes only `updated_at >= cursor`,
   advancing the cursor each pass.
3. Loops until it **converges** — and here is a subtlety this lab hit for real
   the first time it ran: convergence must be measured in **time, not doc
   count**. Each pass re-copies the overlap window, so at steady state a pass
   copies `write_rate × (overlap + pass_interval)` docs and a "fewer than N
   docs" check may never fire. What you actually need to bound is the duration
   of the final write-blocked pass, so the correct criterion is "a full pass
   now completes in a few seconds." If passes stop getting faster, your write
   rate exceeds your copy rate — throttle writes or scale up; the script
   aborts rather than looping forever.

Why the **overlap margin** (5s here)? Clock skew between app nodes (it is the
*client* that stamps `updated_at`), and refresh lag (a doc indexed at T may
not be searchable until T+refresh). Re-copying a doc twice is harmless — same
`_id`, whole-document overwrite, idempotent. Missing a doc is not harmless.
When in doubt, overlap. Size it to skew + refresh, not to paranoia: every
second of overlap adds `write_rate × 1s` docs to every pass forever.

`conflicts=proceed` matters too: if a live write bumps a doc's version while a
catch-up pass is copying it, the version conflict is skipped — correct, because
the *next* pass (or the final pass) will pick up that newer write anyway.

### 3.3 The cutover: why "zero write downtime" is (almost) a lie

Here is the part most blog posts hand-wave. With timestamp catch-up alone,
there is always a race: a write can land on v1 **after your final catch-up
pass read the data but before the alias swap**. That write is lost. You cannot
have all three of:

1. no write pause,
2. no dual-write in the application,
3. zero data loss.

Pick two. The three honest designs:

| Design | Write availability | App changes | Guarantees | Used by |
|---|---|---|---|---|
| **Brief write block** (this lab) | ~1–2s of retried writes | none (writers must retry — they should anyway) | zero loss, provable | gh-ost/pt-osc cutover, most ES migrations |
| **App dual-write** during final window | 100% | dual-write + failure handling on the write path | zero loss *if* dual-write failure handling is correct (it's subtle: partial failures, ordering) | large orgs with migration tooling |
| **Accept the gap** + async repair | 100% | none | eventual repair via one post-swap sweep; brief staleness window | read-mostly systems |

This lab implements the write block: `index.blocks.write=true` on v1 → refresh
→ final delta pass → **atomic alias swap** → unblock. The traffic generator
demonstrates the client side of the contract: it retries `cluster_block_exception`
with backoff. Reads are never interrupted for even a millisecond.

A lesson this lab learned the hard way: the first run measured a **6.66s**
block window even though the final pass copied only 366 docs — because the
orchestrator polled the reindex task every 3 seconds. **The block window
includes your orchestration overhead, not just the copy.** Everything inside
the blocked section must be tight: fast task polling (250ms here), no lazy
sleeps, no logging round-trips. After the fix the window is dominated by the
actual copy + refresh (~1–2s at this write rate).

> Interview-grade insight: this is the same shape as every online migration
> ever. gh-ost briefly locks for the table rename. Blue-green deploys have a
> connection-drain window. The skill is not eliminating the pause — it is
> making it *seconds*, making it *safe*, and making clients *tolerate* it.

### 3.4 Deletes — the silent killer of catch-up migrations

Timestamp catch-up copies documents that *exist*. A **hard delete on v1 leaves
no row to copy** — v2 resurrects the deleted doc. This lab side-steps it the
way mature systems do: **soft deletes** (`is_deleted: true` + `updated_at`
bump), which propagate like any update, filtered out at query time and purged
by a periodic janitor job.

If you are stuck with hard deletes, your options are: dual-delete from the app
during the migration window, or CDC-based replication (Lab 03) where deletes
are first-class events. Know this trade-off cold — it is *the* classic
follow-up question.

### 3.5 Verify, or it didn't happen

"The migration succeeded" is a claim, not a fact. `verify.py` runs three
independent checks:

1. **Journal replay** — the traffic generator journals every *acknowledged*
   write. Verification `mget`s every journaled id from the live alias and
   asserts it exists with `updated_at >=` the journaled value. An acked write
   that is missing is a data gap, full stop. This is the strongest check
   because it tests the user-visible contract, not internal state.
2. **Count reconciliation** — `seeded + created == live count` (soft deletes
   keep counts stable, deliberately).
3. **Content sampling** — random docs compared field-by-field across v1/v2 to
   catch mapping-conversion corruption (e.g. `float → scaled_float` precision).

In production add: aggregate checksums per time bucket, and a shadow-read
phase (read from both, compare, serve old) before trusting v2.

## 4. Runbook (step by step)

```bash
cd labs/01-es-zero-downtime-reindex

# 0. One command to see the whole story:
make demo

# --- or step by step: ---
make up install          # 1. start ES, install deps
make bootstrap           # 2. create products_v1 + both aliases
make seed                # 3. load 300k docs (SEED_DOCS=3000000 make seed for real pain)
make traffic-start       # 4. live writes begin (watch: tail -f traffic.log)
make reindex             # 5. the migration — watch every phase log
make traffic-stop        # 6. stop traffic
make verify              # 7. ✅ prove zero data gap
```

What you should observe during `make reindex`:

- Baseline copy throughput in docs/s, then each catch-up pass copying fewer
  docs than the previous one.
- `traffic.log` showing a handful of `Write p-XXXX delayed 1.4s` warnings at
  cutover — that is the write block being absorbed by client retries.
- `Write block window: ~1-2s`, and reads (try `curl localhost:9200/products-read/_count`
  in a loop) never failing.

### Failure drills

```bash
# Drill 1: rollback after cutover (replaying v2-era writes back into v1)
python3 scripts/rollback.py --since-ms <cutover_epoch_millis>
make verify              # journal check still passes against v1

# Drill 2: kill -9 the orchestrator mid-baseline; rerun after cleanup
curl -XDELETE localhost:9200/products_v2   # idempotent restart from scratch
make reindex

# Drill 3: non-convergence — crank traffic (edit sleep in traffic.py to 0),
# watch the orchestrator abort instead of looping forever.
```

## 5. Production checklist

Everything that changes between this lab and 500M docs on a real cluster:

- [ ] **Throttle**: always set `--rps`; watch search p99 during the copy.
      Run the baseline during low-traffic hours.
- [ ] **Disk**: you need ~2× the index size free *during* the migration
      (old + new + merge headroom). Check before starting, not during.
- [ ] **Snapshot first**: take a snapshot before cutover. Rollback via alias
      is instant; rollback via snapshot restore is your last resort.
- [ ] **Alias discipline**: enforce "no concrete index names in app config"
      with a linter/convention, or this whole playbook is unavailable.
- [ ] **Writers retry**: audit every write path for retry-with-backoff on
      429/blocked. This is the contract that keeps the cutover safe.
- [ ] **Deletes**: soft-delete policy in place, or dual-delete plan written.
- [ ] **Monitoring during migration**: reindex task progress, target index
      indexing rate, cluster heap/GC, search latency, rejected threadpool.
- [ ] **Keep v1** until verification passes *and* a soak period (24–72h)
      elapses. Disk is cheaper than data loss.
- [ ] **Replicas after, not during**: `number_of_replicas=0` for the copy,
      restore afterwards and wait for green before declaring victory.
- [ ] **Automate the whole runbook**: a migration you can't rerun identically
      is a migration you can't roll forward. Scripts, not shell history.
- [ ] For **continuous** migration needs (not one-shot), graduate to CDC
      (Lab 03): the source of truth streams changes; ES is a projection.

## 6. Interview questions to answer without notes

1. Why must the alias swap be atomic, and what breaks if you swap read and
   write aliases in two separate calls?
2. Walk through the exact race that loses a write if you skip the write block.
   Where are the two other places (besides blocking) you could close it?
3. Why is the catch-up overlap margin needed even with NTP-synced clocks?
4. How do hard deletes break timestamp catch-up, and what are three fixes?
5. Your catch-up passes stopped shrinking. What are the possible causes and
   what do you do?
6. After cutover, verification fails: 12 journaled writes are missing. What is
   your immediate action, and what do you investigate before retrying?
7. Why `refresh_interval=-1` and `replicas=0` during the copy — and what is
   the risk of `replicas=0` if a node dies mid-migration?

## 7. FAQ deep dive — one alias vs. two

*This section grew out of real questions asked while building the lab. They are
exactly the questions a good reviewer will ask about your design.*

### Q1. Why two aliases? My application uses a single alias.

Honest answer first: **for the cutover strategy in this lab, one alias is
enough.** Both aliases always move together in the same atomic `_aliases`
call, so a single alias would behave identically:

```python
es.indices.update_aliases(actions=[
    {"remove": {"index": "products_v1", "alias": "products"}},
    {"add":    {"index": "products_v2", "alias": "products"}},
])  # still atomic
```

Two aliases are not required for *this* migration — they are **option value**:
they let you express intermediate states where the read target and write
target differ. One alias has exactly two states (`→ v1` or `→ v2`); two
aliases turn the migration into a state machine:

| Phase | `products-read` | `products-write` | Why you'd want it |
|---|---|---|---|
| Normal | v1 | v1 | |
| **Read-first cutover** | **v2** | v1 (+ catch-up loop) | Soak v2 under real search traffic — latency, relevance, mapping bugs — while reverting reads stays a free one-call operation |
| Cutover complete | v2 | v2 | |
| **Reads-only rollback** | v1 | v2 | v2 serves bad/slow results but data is fine: rescue UX instantly, no reverse catch-up needed |

The read-first phase is the valuable one. Real ES migration incidents are
rarely lost data — they are *search quality* regressions on the new mapping
(wrong analyzer, relevance drop, latency blow-up from edge-ngrams), and those
only surface under real read traffic. With one alias, the first moment v2
sees real reads is also the moment you are all-in.

### Q2. If I use one alias, do the migration steps change?

No. Baseline copy, catch-up loop, convergence check, write block, final pass,
atomic swap, verification, rollback — all identical. The only difference is
swapping one alias instead of two. What you give up is the intermediate
states in the table above, not safety of the big-bang path.

### Q3. Doesn't two aliases force complex if/else routing in the app?

No — this is the common misconception. Routing does not depend on any runtime
condition; it depends on the **operation type**, which is known statically at
the call site. `search`/`mget`/`count` are always reads; `index`/`update`/
`delete` are always writes. There is no branch to write — it is **two config
constants instead of one**, applied in the repository layer you (should)
already have:

```python
class ProductRepo:
    def search(self, query):
        return es.search(index=ES_READ_ALIAS, query=query)

    def save(self, doc_id, doc):
        return es.index(index=ES_WRITE_ALIAS, id=doc_id, document=doc)
```

This is the same shape as database read-replica routing: nobody writes
if/else for primary-vs-replica either — write repos point at the primary,
read repos at the replica.

### Q4. Full trade-off analysis

**What two aliases buy you:**

1. **Cutover as a state machine** (Q1) — read-first canary, gradual rollout.
2. **Rollback granularity** — revert reads without reverse-syncing writes.
3. **Read-side filters and fan-out** — a read alias can carry a filter (e.g.
   `is_deleted: false`, making soft-delete invisibility an infrastructure
   concern instead of a per-query one) and can span multiple indices
   (time-based pattern: `logs-read` → 12 monthly indices, `logs-write` → the
   current one). Both are possible with one alias but semantically trappy:
   writes through a filtered alias silently ignore the filter, and writes
   through a multi-index alias are rejected unless `is_write_index` is set.
4. **Least privilege** — search service gets read-alias permissions only,
   ingest workers write-alias only. A compromised search path cannot corrupt
   data.
5. **Observability** — slow logs and metrics split cleanly by path.

**What two aliases cost you (the honest list):**

1. **Silent misroute risk — the dangerous one.** If a developer writes
   `es.index(index=READ_ALIAS, ...)`, ES *accepts it* (writing through a
   single-index alias works). The bug runs correctly for months and detonates
   mid-migration, the moment the two aliases point at different indices.
   Antidotes: (a) split permissions so misroutes fail on day one, (b) funnel
   all ES access through one repository layer and lint against direct client
   use, (c) in tests, point the read alias at two indices so writes through
   it fail fast.
2. **Permanent cognitive overhead.** Every call site picks one of two
   constants; every new engineer asks why. Small, but multiplied by codebase
   lifetime.
3. **Framework friction.** Spring Data ES and some ODMs accept a single
   `indexName` per entity. Forcing two aliases through them costs you
   framework convenience — *this* is where real complexity appears, not in
   routing logic.
4. **Phantom flexibility.** If you never canary, never split permissions,
   never fan out — you prepaid for an option you never exercise.

**Decision matrix:**

| Context | Recommendation |
|---|---|
| Small/medium index, rare migrations, seconds of write-block acceptable | **1 alias** — don't pay for options you won't use |
| Search is a revenue path, frequent mapping changes, need canary/soak | **2 aliases** |
| Multi-tenant / security-sensitive, least privilege required | **2 aliases** (permissions alone justify it) |
| Framework hard-codes one index name | **1 alias**, unless security forces the issue |
| No repository layer around ES yet | Build that first — it's the prerequisite that makes 2 aliases cheap |

**The verdict that matters:** the load-bearing decision is *alias instead of
concrete index name* — that one cannot be deferred. Going from one alias to
two later is a reversible, low-cost migration (add the new alias pointing at
the same index, flip constants call-site by call-site) — **provided** all ES
access goes through a single repository layer. That discipline is what keeps
today's simple choice from locking you in tomorrow.

## 8. File map

```
docker-compose.yml     ES 8.14 single node (+ kibana under --profile ui)
mappings/v1.json       original mapping (sku:text, price:float)
mappings/v2.json       target mapping (sku:keyword, autocomplete analyzer, scaled_float)
scripts/common.py      config, client, alias resolution
scripts/bootstrap.py   create v1 + aliases (the load-bearing indirection)
scripts/seed.py        parallel bulk load with load-time tuning
scripts/traffic.py     live writes + retry-on-block + acked-write journal
scripts/reindex.py     the orchestrator: baseline -> catch-up -> cutover
scripts/verify.py      journal replay + counts + content sampling
scripts/rollback.py    reverse catch-up + atomic swap back
```
