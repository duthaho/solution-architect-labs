# Lab 11 — Concurrent money transfer / double-spend (P04)

## What & why

The classic fintech interview scenario made runnable: an account holds 50k,
two devices each send 50k at the same moment, and with naive
read-modify-write both succeed — money is created or destroyed. The lab
reproduces the lost-update / double-spend **deterministically**, then fixes it
four ways and measures the trade-offs under a hot-account workload.

Graduates backlog item **P04** to `labs/11-concurrent-transfers`.

## Decisions

- **D1 — Four runnable strategies**, one script each:
  - **a — pessimistic:** `SELECT ... FOR UPDATE`, locks taken in sorted
    account-id order, deadlock-retry wrapper.
  - **b — optimistic:** `version` column, `UPDATE ... WHERE id=? AND
    version=?`, retry loop on 0 rows matched.
  - **c — atomic conditional:** single `UPDATE accounts SET balance =
    balance - ? WHERE id = ? AND balance >= ?`; no explicit read.
  - **d — append-only ledger:** balances are `SUM(amount)` over an
    `entries` table; overdraft prevented by inserting under a per-account
    serialization point; a materialized balance cache shows the read-path
    cost.
- **D2 — SERIALIZABLE isolation is deep-dive text only** ("road not taken"),
  not a runnable strategy.
- **D3 — Two extra drills beyond the core race:**
  - **Core drill:** N workers race transfers on a tiny hot account set with
    the naive handler → conservation violated; same drill under each
    strategy → conserved.
  - **Deadlock drill:** A→B and B→A concurrently with FOR UPDATE in naive
    (arrival) lock order → MySQL 1213; rerun with sorted-order locking →
    zero deadlocks. (Overdraft racing is asserted inside the core drill and
    verify, not a separate drill.)
- **D4 — `make bench` included:** identical hot-account contended workload
  across all four strategies; table prints throughput, p50/p95 latency,
  retry/deadlock counts, and a conservation check per strategy.
- **D5 — Repo conventions apply:** MySQL 8.0.43 single node, Python 3.11 +
  PyMySQL only (no ORM), Makefile targets, `.jsonl` outcome journals,
  `verify.py` invariant checker, 7-section README, root README table +
  BACKLOG.md flipped to ✅ Covered on ship.

## Assumptions

- **A1 —** Schema: `accounts(id, balance DECIMAL(18,2), version INT)` seeded
  with a small hot set (default 10 accounts, env-tunable) plus a `transfers`
  journal table; strategy d adds `entries` (append-only, `(account_id, seq)`
  unique). Balances always non-negative is a schema-level invariant of the
  lab (business rule: no overdraft).
- **A2 —** The naive baseline is REPEATABLE READ read-modify-write in a
  transaction — showing that "it's in a transaction" does **not** prevent
  lost updates is the pedagogical core of §1.
- **A3 —** Conservation invariant checked by `verify.py`:
  `SUM(balance) == seeded total` **and** no balance < 0 **and** every acked
  transfer in the journal is reflected exactly once (ledger strategy: cache
  == SUM(entries)). Non-zero exit on violation.
- **A4 —** Determinism: drills use a barrier (all workers connect, then
  fire together) and enough iterations that the naive race reproduces every
  run; MySQL port 3317 (`MYSQL_PORT` override), db `lab11`.
- **A5 —** Demo runtime target: full `make demo` ≤ ~5 minutes on a laptop;
  bench sized accordingly (env-tunable ops count).

## Out of scope

- Distributed transactions / cross-database transfers, sagas, 2PC.
- Idempotency keys for client retries (lab 08's territory; a README pointer).
- SERIALIZABLE as runnable code (D2).
- Multi-node MySQL, replication interaction with locking.

## Acceptance criteria

1. `make demo` runs end-to-end from a clean checkout: up → bootstrap →
   seed → naive race (violation shown) → 4 strategies (conserved) →
   deadlock drill → verify → bench.
2. The naive drill **deterministically** shows a conservation violation
   (lost update / double-spend) every run, printed with the exact numbers.
3. Each of the four strategies passes the same drill with money conserved
   and no negative balances.
4. Deadlock drill shows real 1213 deadlocks with naive lock order, zero
   with sorted order.
5. `make verify` exit 0 after strategy runs; exit 1 when pointed at the
   naive run's outcome.
6. `make bench` prints the comparison table (D4).
7. `make clean` returns the machine to pristine.
8. README follows the 7-section convention; root README table gets lab 11;
   BACKLOG.md P04 flipped to ✅ Covered.

## End-to-end check

`cd labs/11-concurrent-transfers && make demo` — full chain above, then
`make verify` exit 0, `make clean`.
