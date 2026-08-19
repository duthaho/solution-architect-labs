# Lab 11 — Concurrent money transfer: the double-spend

An account holds 50k. Two devices tap "send 50k" at the same moment. Both
requests read the balance, both see enough money, both commit — and the bank
just paid out 100k against a 50k account. This lab reproduces that lost
update **deterministically** (not "run it a few times and hope"), proves that
wrapping the code in a transaction does not help, then fixes it four
different ways and benchmarks them against each other on a deliberately hot
account.

Everything runs on one MySQL container. `make demo` is the whole story:
bug → checker catches it → deadlock detour → four fixes → invariants → bench.

## 1. The problem

The naive transfer handler looks completely reasonable, and it is the one in
most codebases:

```
BEGIN;
SELECT balance FROM accounts WHERE id = :src;   -- 1000, plenty
-- app code: check balance >= amount, compute fees, call fraud service…
UPDATE accounts SET balance = 200  WHERE id = :src;  -- 1000 - 800, computed in app
UPDATE accounts SET balance = 1800 WHERE id = :dst;
COMMIT;
```

The trap has three teeth:

* **"It's in a transaction" is not a defense.** Under REPEATABLE READ (the
  InnoDB default) a plain `SELECT` reads a snapshot and takes **no lock**.
  Two racers read the same snapshot, both pass the balance check, and the
  second `COMMIT` silently overwrites the first one's debit. No error, no
  rollback, no log line — the money is just wrong.
* **The corruption is silent and asymmetric.** Each written value is
  individually plausible (`>= 0`, derived from a real read), so no
  constraint fires. When the racers credit different destinations, the
  debits collapse into one while every credit lands: money is *created*.
  That is the double-spend — the same 1000 funded 1600 of payouts.
* **You cannot catch it by staring at latencies or error rates.** The drill
  in this lab makes it visible the only way it ever is in production: an
  accounting sweep that sums balances against what was acked
  (`scripts/verify.py`) — which is why real money systems run exactly such
  sweeps continuously.

Which way would you rather be wrong? A transfer that fails loudly and can be
retried, or one that succeeds quietly and mints money? Every fix in this lab
is a different answer to *where the check-and-debit becomes atomic*.

## 2. Architecture

```
                       drill_race.py
        8 workers × N rounds, barrier-released together;
        every round ALL workers debit the SAME hot account,
        each credits a different dst with a different amount
                             │
              fn(conn, transfer_id, src, dst, amount)
                             │
     ┌──────────┬────────────┼─────────────┬─────────────┐
   naive        a: FOR       b: version    c: atomic     d: append-only
   read/hold/   UPDATE,      column,       conditional   ledger + cache
   write        sorted ids   retry loop    UPDATE        (entries = truth)
     │            │            │             │             │
     ▼            ▼            ▼             ▼             ▼
  ┌──────────────────────────────────┐   ┌───────────────────────────┐
  │ accounts(id, balance, version)   │   │ entries(account_id, seq,  │
  │ transfers(transfer_id, mode, …)  │   │         amount, tid)      │
  │   ← applied-work journal, joined │   │ balance_cache(account_id, │
  │     against race_<mode>.jsonl    │   │         balance, last_seq)│
  └──────────────────────────────────┘   └───────────────────────────┘
                             │
                        verify.py
        conservation · no negatives · journal-implied balances
        · exactly-once (transfer_id join) · cache == SUM(entries)
```

Contracts the whole lab hangs on:

* **Conservation of money.** Transfers move money; they never create or
  destroy it. `SUM(balance)` must equal the seeded total forever.
* **No overdraft.** The business rule is `balance >= 0` — enforced by each
  strategy in flight (or not, that's the bug), and by `verify.py` after the
  fact. There is deliberately **no** DB CHECK constraint: MySQL 8 enforces
  CHECK, and a DB-level floor would change the naive failure mode we're here
  to observe.
* **Every acked transfer exists exactly once.** Each handler writes its
  `transfers` row inside the same transaction as the balance change; the
  client journals every ack to `race_<mode>.jsonl`. `verify.py` joins the
  two on `transfer_id`, both directions.
* **The race is deterministic by construction.** The naive handler holds
  `HOLD_MS` (default 50ms) between read and write — modeling the app think
  time (fees, fraud check, an RPC) every real transfer path has — so
  concurrent read-windows always overlap. And workers use *distinct* amounts
  and destinations, because symmetric lost updates cancel out in the sum and
  hide the bug.

## 3. Deep dive: four places to put the atomicity

### 3.1 The naive handler, and why REPEATABLE READ doesn't save it

`transfer_naive` is a read → check → think → write of **computed literal
values**. The snapshot read means both racers compute from the same stale
balance; the last COMMIT wins and the other debit evaporates. Isolation
levels are about what you can *read* — lost updates are about what you
*write* based on stale reads. Run `make drill-race MODE=naive` and watch the
drift: with 8 workers the first run typically creates well over +1500 out of
thin air, every single run.

### 3.2 Strategy a: pessimistic — `SELECT … FOR UPDATE` in sorted order

Lock both account rows before reading, always in **sorted id order**. The
check now reads a current, locked row; the race window is gone because
contenders queue at the lock. The two costs: throughput under contention is
whatever the lock queue allows, and lock *ordering* becomes a correctness
property of every code path that touches two accounts — `make
drill-deadlock` shows opposing transfers deadlocking (error 1213) the moment
one path locks in arrival order. Retry-on-1213 stays anyway: InnoDB can pick
you as a victim for reasons you don't control.

### 3.3 Strategy b: optimistic — a `version` column

Read without locks, then make every write conditional:
`UPDATE … SET balance = ?, version = version + 1 WHERE id = ? AND version = ?`.
If `rowcount != 1`, someone got there first → roll back, re-read, retry. The
stale write can *never land*; you pay in retries instead of locks. The bench
makes the trade brutal and visible: on a single hot account, 8 workers burn
~850 retries for ~240 acked transfers and finish last. Optimistic locking is
the right tool for *low-contention* rows, which a hot corporate settlement
account is not.

### 3.4 Strategy c: atomic conditional — the debit is the check

```sql
UPDATE accounts SET balance = balance - :x WHERE id = :src AND balance >= :x
```

No read at all. InnoDB evaluates the predicate on the current locked row, so
there is no snapshot to go stale; `rowcount = 0` *is* the insufficient-funds
answer, atomically. Smallest diff, fastest under contention (it takes the
row lock for the shortest possible time), and the interview answer most
often missed. Its limit: business logic that needs the read value — tiered
fees, limits, fraud scoring — can't be folded into one UPDATE's predicate,
which is exactly when you reach back to a or b.

### 3.5 Strategy d: the ledger — stop updating balances at all

Banks don't run `UPDATE balance`. In `transfer_ledger` the truth is
`entries`: signed, append-only, auditable rows (opening balance is entry
seq 1, written by seed). A transfer is a debit+credit pair; `balance_cache`
is a materialized read model whose row locks (taken in sorted order, like a)
serialize each account and hand out the next `seq`; the `(account_id, seq)`
primary key is the backstop that even a buggy writer cannot double-append.
You gain history, auditability and natural idempotency hooks; you pay write
amplification (2 entries + 2 cache updates + 1 transfers row per transfer)
and a read path that must trust the cache — which is why `verify.py`
reconciles `balance_cache == SUM(entries)` per account.

**The road not taken: SERIALIZABLE.** Setting the isolation level to
SERIALIZABLE makes InnoDB promote plain SELECTs to shared locks, and the
naive handler's race becomes a deadlock/lock-wait error instead of silent
corruption. It is a real fix — and this lab still doesn't implement it,
because it is strategy a with the locks chosen implicitly: every read in the
transaction gets locked whether it needs it or not. You keep the retry
loop (errors are now the *success path* signal), lose the ability to
reason about which rows are contended, and pay for it across the entire
workload, not just transfers. Know it for the interview; reach for explicit
locks in code.

## 4. Runbook (step by step)

```bash
cd labs/11-concurrent-transfers
make demo          # the whole story, ~45s on a laptop
```

Or piece by piece:

```bash
make up install bootstrap seed   # MySQL on :3317, schema, 10 accounts × 1000.00
make drill-race MODE=naive       # the bug, deterministically
make verify-naive                # the checker MUST fail here (inverted exit)
make drill-deadlock              # arrival-order locks deadlock; sorted don't
make seed                        # reset the corrupted world
make drill-race MODE=a           # …then b, c, d — same drill, money conserved
make verify                      # all invariants, exit 0
make bench                       # the comparison table
make clean                       # back to pristine (containers, volumes, journals)
```

Expected shape of the interesting moments (numbers vary slightly):

```
--- drill_race MODE=naive (8 workers x 12 rounds, hot set 4) ---
acked=88 rejected=8 retries=0 wall=1.4s
start_total=10000.00 end_total=11764.00 drift=+1764.00
VERDICT: CONSERVATION VIOLATED — lost update / double-spend reproduced

arrival lock order: 10 opposing rounds -> acked=10 deadlocks(1213)=10
 sorted lock order: 10 opposing rounds -> acked=20 deadlocks(1213)=0

strategy                      acked  rejects  retries    ops/s   p50 ms   p95 ms  conserved
a pessimistic (FOR UPDATE)      243       77        0     85.9     46.0     89.2        yes
b optimistic (version)          243       77      860     72.6     59.5    105.9        yes
c atomic conditional            244       76        0    114.9     35.5     65.4        yes
d append-only ledger            243       77        0     92.5     43.3     82.6        yes
```

Knobs: `WORKERS`, `ROUNDS`, `HOT`, `HOLD_MS`, `BENCH_ROUNDS`, `ACCOUNTS`,
`BALANCE`, `MYSQL_PORT` — all env vars. `make up MYSQL_PORT=3399` if 3317 is
taken; add `--profile ui` in compose for adminer on :8091.

## 5. Production checklist — what changes with real money and a pager

* **Run the accounting sweep in production, continuously.** verify.py is a
  toy version of the reconciliation job every payment system runs. The naive
  bug ships to prod regularly; the sweep is how you find it in hours instead
  of at audit time.
* **Idempotency keys before concurrency fixes.** This lab's clients never
  retry an ack. Real clients retry on timeout, and a retried transfer is a
  double-spend that no locking strategy prevents — `transfer_id` as a
  client-generated unique key (lab 08's territory) is the other half of
  correctness.
* **Hot accounts are a product problem too.** One celebrity merchant account
  serializes every strategy here. Real systems shard hot accounts into
  sub-balances, batch small credits, or move the hot path to the ledger
  model where appends don't contend on one row.
* **Watch retry storms, not just p95.** Strategy b's 850 retries were
  invisible in acked counts. Under real load that's connection-pool
  exhaustion and a metastable failure — cap retries, add jitter, and alert
  on retry *rate*.
* **DECIMAL, never float; and mind the isolation default.** Everything here
  is DECIMAL(18,2). And if someone "fixes" a replica-lag issue by dropping
  to READ COMMITTED, nothing in the naive handler gets better — the lost
  update is isolation-level-independent.
* **The ledger wins at scale for reasons this lab can't show:** regulatory
  audit, dispute resolution, temporal queries ("balance as of March 31"),
  and event-sourcing downstream consumers. The write amplification is the
  fee, not the verdict.

## 6. Interview questions to answer without notes

1. Two devices send 50k from a 50k account simultaneously and both succeed.
   Walk through the exact interleaving that makes this happen inside
   transactions.
2. Why doesn't REPEATABLE READ prevent the lost update? What *would*
   SERIALIZABLE do to this workload, and what's the cost?
3. `SELECT … FOR UPDATE` on both accounts: why must the lock order be
   deterministic, and what exactly happens when it isn't?
4. When is optimistic locking the wrong choice? What metric tells you?
5. Why is `UPDATE … WHERE balance >= amount` immune to the race without any
   explicit lock? What class of business logic breaks this pattern?
6. In the ledger model, what serializes two concurrent transfers touching
   the same account? What stops a buggy writer from appending two entries
   at the same seq?
7. The balance cache drifted from SUM(entries). How did it happen, how do
   you detect it, how do you repair it online?
8. A client times out and retries a transfer that actually committed. Which
   of the four strategies protects you? (Trick question — none. What does?)
9. Your accounting sweep says SUM(balances) is 1764 over the seeded total.
   What do you look at first, and what does "asymmetric lost update" mean
   for where the money came from?
10. The product team ships a "send to 1000 recipients" feature. Which
    strategy survives, and what new failure mode appears?

## 7. File map

| File | What it does |
|------|--------------|
| `docker-compose.yml` | MySQL 8.0.43 on :3317 (+ optional adminer UI on :8091) |
| `sql/schema.sql` | accounts, transfers (applied-work journal), entries + balance_cache (ledger) |
| `scripts/common.py` | config, connections, jsonl helpers, mode-aware balance readers |
| `scripts/bootstrap.py` | applies the schema |
| `scripts/seed.py` | world reset: N accounts × opening balance, seed.json baseline, journals cleared |
| `scripts/strategies.py` | the five handlers: naive, pessimistic, optimistic, atomic, ledger |
| `scripts/drill_race.py` | the core drill: barrier-released hot-account race; exit code asserts the mode's expectation |
| `scripts/drill_deadlock.py` | opposing transfers, arrival vs sorted lock order |
| `scripts/verify.py` | invariant gate: conservation, negatives, exactly-once, cache reconciliation |
| `scripts/bench.py` | four-way comparison under identical contention |
| `Makefile` | `demo` runs the whole story; every step available à la carte |
