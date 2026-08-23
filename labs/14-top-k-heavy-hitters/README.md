# Lab 14 — Top-K most-viewed products (heavy hitters, approximately)

10M products, 10B views/day, and one dashboard question: *what are the
top-100 right now — per minute, per day, per month?* This is the heavy
hitters problem, and it is the repo's first lab where the invariant is not
consistency but **bounded, measured error**: sketches trade exactness for
fixed memory and mergeability, and the lab measures precisely what that
trade costs — including the cliff where an undersized sketch quietly starts
lying.

## The problem

The naive answer is a hash map: `counts[product] += 1`, sort, take 100. At
laptop scale it even works — that is the first honest lesson here, and the
`exact` contender stays in every table as the oracle. What kills it in
production is multiplication, not size:

- **windows** — per-minute top-K over a day is 1,440 separate counting
  problems, per month 43,200; each window holds its own key space
- **shards** — every partition counts its own traffic; the day's top-100
  needs those partial results *combined*, and hash maps combine by
  re-shipping every key
- **unbounded cardinality** — 10M products today, plus bots, plus SKU
  churn; the map grows with the data, the sketch does not

The trap inside the trap: the day's #3 product can be one that never once
appeared in a minute's top-100 — steady sellers lose every sprint and win
the marathon. Any design that keeps only per-window top-K lists and unions
them later is silently wrong, and this lab reproduces that miss
deterministically.

## Architecture

```
              one seeded Zipf stream (1M events, ~137k distinct keys)
                                   │
     ┌──────────┬──────────┬───────┴───┬────────────┬─────────────┐
     ▼          ▼          ▼           ▼            ▼             ▼
  exact       CMS        CMS-CU    Space-Saving  Redis TOPK   MySQL rollup
  dict     +top-K heap  (conserv.)  (m=4096)    (HeavyKeeper) (per-minute
 (oracle)  w: 2^17→2^9  w: 2^17→2^9                            exact rows)
     │          │          │           │            │             │
     └──────────┴──────────┴─────┬─────┴────────────┴─────────────┘
                                 ▼
              accuracy_runs.jsonl / merge_runs.jsonl   (evidence journals)
                                 ▼
              verify.py — replays the stream from its seed and RECOMPUTES:
              one-sided error, guarantees, merge identity, recall floors
              (VERIFY_INVERT=1 proves the gate catches the starved sketch)
```

| contender | mechanism | error contract |
|---|---|---|
| `exact` | hash map | none — the oracle |
| `cms` | Count-Min Sketch + size-K heap | overestimates only, ≤ εN w.p. 1−δ |
| `cms_cu` | CMS, conservative update | same bound, ~2× tighter in practice, **loses mergeability** |
| `spacesaving` | m counters, evict-min | deterministic: every key with f > N/m is present |
| `redis_topk` | HeavyKeeper (fingerprint + decay) | can *under*estimate; top-K only, highest precision per byte |
| `mysql_rollup` | `INSERT..ON DUPLICATE KEY UPDATE` per minute | exact, durable, windowed — and the slowest writer |

## Deep dive

### Count-Min Sketch, and what εN really means

A CMS is a `d × w` grid of counters with one hash per row: update increments
one cell per row, query takes the **min** across rows. Collisions only ever
add, so the estimate is never below the truth (one-sided error) — the drill
asserts this on every journaled evidence pair. With `w = ⌈e/ε⌉, d =
⌈ln(1/δ)⌉` the overestimate stays under **εN** with probability 1−δ
([Cormode & Muthukrishnan 2005](https://dimacs.rutgers.edu/~graham/pubs/papers/cm-full.pdf)).

The part everyone misses: the error is proportional to **N, the whole
stream**, not to the key's own count. A key seen 3 times in a 1M-event
stream can read as 4,813 (the drill's measured `max_err` at width 512) —
every one of its cells collided with elephants. Top-K makes this worse than
point queries: it takes only a handful of inflated cold keys to evict real
mid-rank hitters from the heap.

(A cousin worth knowing but not implemented here: the **Count Sketch**
[Charikar et al. 2002] adds a ±1 sign hash and takes the *median* instead
of the min — unbiased, two-sided error bounded by the L2 norm instead of
εN, which behaves much better under heavy skew at the cost of more rows.)

### The cliff, measured

One seeded stream, widths halving from generous to starved
(`make drill-accuracy`, deterministic under `SEED=14`):

```
contender        width recall@100  rank_ovl  mean_err  max_err
cms             131072      1.000     1.000       0.1        1
cms_cu          131072      1.000     1.000       0.0        0
cms              32768      1.000     1.000       3.0       10
cms_cu           32768      1.000     1.000       0.0        0
cms               8192      1.000     0.998      21.8       36
cms_cu            8192      1.000     1.000       0.0        0
cms               2048      0.990     0.995     136.9     1073
cms_cu            2048      0.990     0.999       9.0      899
cms                512      0.550     0.781    1603.0     4813
cms_cu             512      0.630     0.811     736.9     3957
spacesaving     m=4096      1.000     1.000       0.0        0
redis_topk          hk      1.000     0.999       1.5       10
mysql_rollup       sql      1.000     1.000       0.0        0
```

Recall holds at 0.99+ down to 2,048 counters per row, then falls off a
cliff to 0.550 at 512. Note the shape: `mean_err` grows smoothly (3 → 22 →
137 → 1,603) but recall fails suddenly — overestimation is survivable while
it's uniform, and fatal the moment cold-key estimates cross the top-100
boundary. **Conservative update** (only raise the cells that equal the
current min) buys roughly 2× less overcounting and holds 0.630 where
vanilla holds 0.550 — it moves the cliff, it does not remove it.

### Space-Saving: the deterministic counter

Space-Saving ([Metwally et al. 2005](https://www.cs.ucsb.edu/research/tech-reports/2005-23))
keeps exactly m counters. A new key evicts the smallest counter and
*inherits its count* — so tracked counts overestimate by at most the
inherited floor, and anything with true frequency above N/m is guaranteed
present, no probability involved. At m=4,096 (400KB) it scores a perfect
recall on this stream; skew is its friend, since elephants entrench early.
This determinism is why the Misra-Gries family is the industrial default
for "give me the frequent items" ([Cormode & Hadjieleftheriou, VLDB
2008](http://www.vldb.org/pvldb/vol1/1454225.pdf); Apache DataSketches'
frequent-items sketch is this family).

### HeavyKeeper: what Redis actually runs

`TOPK.*` in Redis (core since 8.0 — the lab's compose proves it on plain
`redis:8.0.3`) is not CMS: it's
[HeavyKeeper](https://www.usenix.org/conference/atc18/presentation/gong) —
buckets hold a *fingerprint and a counter*, and a colliding key **decays**
the incumbent's counter with probability `b^-count`. Mice get actively
killed; elephants become nearly immortal (decay probability collapses as
counts grow). The price is inverted error: it can *underestimate* and
answers only top-K, not arbitrary frequencies. On this stream it holds
recall 1.000 in **49KB** — the best precision-per-byte in the table —
which is the paper's claim reproduced at laptop scale. Its decay is
genuinely random, so it is the one contender whose exact error may wobble
between runs (the journals flag it `stochastic`).

### Windows, merging, and the cross-window trap

CMS is a *linear* sketch: two sketches built with the same shape and hash
seeds merge by element-wise addition, and the merge is not approximately
right, it is **bit-identical** to the sketch of the concatenated stream —
`drill-merge` asserts sha256 equality between 60 merged minute-buckets and
one full-stream sketch. That is the whole minute→hour→day story: keep a
sketch per minute, add them.

Two carefully-measured caveats:

1. **Conservative update breaks linearity.** The same 60-bucket
   construction with CU produces a merged sketch that differs from the
   full-stream CU sketch (checksums diverge, asserted). Merged-CU is still
   a valid upper bound, but the CU advantage evaporates — pick CU *or*
   mergeability, not both.
2. **Top-K lists don't merge; candidate sets do.** The drill builds a
   stream where a steady key is ranked ~18th every single minute yet #3
   for the day. Union-of-per-minute-top-10 misses it — deterministically.
   The fix that generalizes: keep top-2K candidates per bucket, merge the
   sketches, re-score the candidate union against the merged sketch. The
   drill shows this recovering the exact day top-10.

### The bench: six ways to count the same stream

300k events, identical stream, each contender using its own natural
batching; the latency sample deliberately includes every batch-flushing
event, so the p95 column shows what the batched contenders actually pay
when the buffer drains (`make bench`; absolute numbers vary by hardware,
the shape does not):

```
contender           ops/s   p50 µs   p95 µs     memory recall@100
exact             1294361      0.4      0.7   5642100B      1.000
cms                109400      5.6      6.4   2097152B      1.000
cms_cu              93265      6.4      7.2   2097152B      1.000
spacesaving        291507      1.2      4.3    409600B      1.000
redis_topk         266212      0.3  17231.6     49328B      0.990
mysql_rollup        75350      0.4  59832.2   7443240B      1.000
```

The dict is fastest *and* exact — at one window on one node, use it; that
is not a concession, it is the sizing lesson. The sketches win elsewhere:
Space-Saving does ~3× CMS throughput in a fifth of the memory (pure-Python
CMS pays 4 hashes per update), and Redis holds the full answer in 49KB that
can be shipped across a network. Read the two batched rows carefully: p50
is a buffered append (0.3–0.4µs) while p95 is the flush itself — 17ms for
a 5,000-item `TOPK.ADD`, 60ms for the MySQL executemany — that is what
"amortized" means when the batch boundary lands on your request. MySQL,
~17× slower than the dict end-to-end, is buying durability and SQL windows,
not speed. At 10B/day the arithmetic is:
116k events/s sustained, so a single Space-Saving instance at bench speed
is already within 3× of the whole feed, and per-shard sketches merged
centrally cover it with room to spare — the merge, not the counting, is
why sketches earn their keep.

## Runbook

```bash
make demo          # the whole story end-to-end (~3 min)

# or step by step:
make up install bootstrap
make selftest          # building blocks + the corrupted-sketch catch
make drill-accuracy    # six contenders vs oracle; the width-sweep cliff
make drill-merge       # linearity, CU divergence, the cross-window trap
make verify-naive      # inverted gate: catches the starved sketch — exit 0
make verify            # invariant gate: replays the stream, recomputes
make bench             # throughput / latency / memory / recall, side by side
make clean
```

Knobs (env): `SEED`, `N_EVENTS`, `N_KEYS`, `ZIPF_S`, `TOP_K`,
`BENCH_EVENTS`, `MERGE_EVENTS`, `MYSQL_PORT` (3320), `REDIS_PORT` (6392).
`make up MYSQL_PORT=... REDIS_PORT=...` if the defaults collide. Adminer UI:
`docker compose --profile ui up -d` → localhost:8094.

## Production checklist

What changes when it's 10B real views and a pager:

- **Size from the boundary, not the average.** The sketch must keep
  cold-key overestimates *below the K-th hitter's count*: estimate the
  top-K boundary from a day of traffic, set εN safely under it, and
  re-check when traffic grows — the cliff arrives silently and recall
  is not a metric your dashboard shows by default. Run a shadow exact
  counter on a sampled slice and alert on measured recall.
- **Fix hash seeds fleet-wide and version them.** Merging requires
  identical (w, d, seeds); a config drift between shards produces merged
  garbage with no error. Treat sketch parameters like a schema migration.
- **Decide merge vs CU per layer.** Per-minute buckets that will be rolled
  up must be vanilla CMS or Space-Saving (mergeable); CU belongs only at
  leaves that are queried directly, never summed.
- **Keep candidates wider than K.** Persist top-2K (or more) per window
  alongside the sketch; the steady-seller trap is a product bug, not a
  theory footnote.
- **Watch HeavyKeeper's contract.** It underestimates and forgets:
  a key evicted during a lull restarts from a decayed floor. For "top-K
  right now" it is superb; for billing or SLAs (exactness contracts) use
  the rollup table — never a sketch (that is lab 08/12 territory: counts
  that move money must be idempotent and exact).
- **Zipf is the friendly case.** Flat traffic (low skew, no clear
  elephants) degrades every top-K structure at once; a skew sweep
  (`ZIPF_S=0.8`) is the first thing to run when a new traffic source
  joins. Parked extensions for this lab: the skew sweep drill and a Redis
  restart/replay drill (TOPK state is in-memory; recovery is replay).

## Interview questions

1. Walk through why a Count-Min Sketch can only overestimate, and what
   single stream property the εN error bound scales with.
2. Your CMS dashboard shows a product with 4,800 views that actually has 3.
   What happened, which knob fixes it, and what does that knob cost?
3. Conservative update halves your error, and your rollup pipeline merges
   minute sketches into hours. Why can't you have both? Prove the merge
   identity for vanilla CMS in one sentence.
4. State Space-Saving's guarantee precisely, and explain why heavier skew
   makes it *more* accurate, not less.
5. How does HeavyKeeper differ from CMS-plus-heap structurally, and why
   does exponential decay give it better precision per byte for top-K
   while making it unusable as a general frequency oracle?
6. Design minute/day/month top-100 for 10B views/day across 40 shards:
   what is stored per shard per minute, what crosses the network, and
   where does the day's answer get computed?
7. A steady product is #3 for the day but never in any minute's top-100.
   Your pipeline unions per-minute top-100 lists — prove it misses, then
   fix it without storing full per-minute counts.
8. When is the exact hash map the *right* answer at 10M keys, and which of
   the three multipliers (windows, shards, cardinality) flips the decision?
9. The recall of your production sketch has never been measured. Describe
   a shadow-verification design that measures it continuously without
   doubling your ingest cost.
10. Why must counts that feed payouts never come from any structure in
    this lab except the rollup table — including one with a deterministic
    error bound?

## File map

| File | What it does |
|------|--------------|
| `docker-compose.yml` | MySQL 8.0.43 (:3320), Redis 8.0.3 (:6392) — TOPK in core, no modules; adminer (:8094, profile `ui`) |
| `Makefile` | every target above; `make demo` is the whole story |
| `sql/schema.sql` | `views_minute` (minute_no, product_id, cnt) — the exact rollup contender |
| `scripts/common.py` | env config, MySQL/Redis clients, seeded Zipf stream, fixed sketch hash params, jsonl journals |
| `scripts/bootstrap.py` | applies the schema (drop + recreate lab14) |
| `scripts/sketches.py` | CMS + conservative update: update/estimate/merge/checksum; selftest with `SELFTEST_BREAK` catch |
| `scripts/topk_stores.py` | Space-Saving and the CMS-backed top-K heap (both with lazy min-heaps); guarantee selftest |
| `scripts/contenders.py` | the six adapters behind one `update(key, minute_no)` / `topk(k)` interface; smoke test |
| `scripts/drill_accuracy.py` | the width-sweep cliff + all-contender accuracy table; journals evidence for verify |
| `scripts/drill_merge.py` | merge linearity (bit-identical), CU divergence, the cross-window trap |
| `scripts/verify.py` | the gate: replays the stream from its journaled seeds and recomputes truth, one-sided error, the SS guarantee, the mysql match, both merges, and the trap (recall metrics are floor-checked); `VERIFY_INVERT=1` |
| `scripts/bench.py` | six contenders, one stream: ops/s, p50/p95, memory, recall |
