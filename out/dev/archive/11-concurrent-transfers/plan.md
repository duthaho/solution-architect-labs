# Plan — lab 11 concurrent transfers (P04)

Template: **lab 10** (single-MySQL compose, venv Makefile, `.jsonl` journals,
verify/bench tail). All paths below are under `labs/11-concurrent-transfers/`
unless rooted.

**Journal contract:** each drill/bench run appends one line per *acked*
transfer to `race_<mode>.jsonl`:
`{"transfer_id","mode","worker","src","dst","amount","ok","reason","retries","ms"}`
— `transfer_id` (uuid) is also a column of the `transfers` table, so
verify.py has an identity to join on [codex F3]. A drill run **truncates its
own mode's journal at start**, and `seed.py` resets the world (truncates
`transfers`/`entries`, deletes `race_*.jsonl`), so journals never span a
reseed [codex F5]. `seed.py` writes `seed.json`
(`{"accounts":N,"total":"<decimal>"}`). `verify.py` reconciles journal +
`transfers` table + balances against `seed.json`.

**Modes:** `naive`, `a` (FOR UPDATE), `b` (optimistic), `c` (atomic
conditional), `d` (ledger). One shared drill harness, strategies as
functions — not five near-identical scripts.

## Tasks

- [x] **T1 — Scaffold** [D5]
  Files: `docker-compose.yml` (mysql 8.0.43, port 3317, db `lab11`,
  healthcheck, named volume), `requirements.txt` (PyMySQL==1.1.1),
  `Makefile` (up/down/clean/install/bootstrap/seed placeholders).
  Verify: `make up install` → container healthy, venv created.

- [x] **T2 — Schema, common, bootstrap** [A1, D5]
  Files: `sql/schema.sql` (`accounts(id, balance DECIMAL(18,2), version
  INT)` — deliberately **no** DB CHECK constraint, so the naive mode's
  negative balance is observable; non-negativity is the app-level rule that
  verify.py enforces [codex F6]. `transfers(transfer_id …)` journal table,
  `entries(account_id, seq, amount)` + `balance_cache` for mode d),
  `scripts/common.py` (env config, connect, jsonl append/read, pctl),
  `scripts/bootstrap.py`.
  Verify: `make bootstrap` then `SHOW TABLES` lists 4 tables.

- [x] **T3 — Seed** [A1]
  Files: `scripts/seed.py` (ACCOUNTS=10, BALANCE=1000.00 env-tunable; writes
  `seed.json`; resets world per journal contract; mode-d gets an **opening
  entry per account** (seq 1 = opening balance) so
  `SUM(entries) == balance_cache` holds from t0 [codex F2]).
  Verify: `make seed` prints account count + SUM(balance) == seed.json total.

- [x] **T4 — Race harness + naive mode (riskiest first)** [A2, A4, D3]
  Files: `scripts/strategies.py` (`transfer_naive`: BEGIN; SELECT both
  balances; **deliberate HOLD_MS pause between read and write** so
  concurrent windows are guaranteed to overlap — determinism by
  construction, not by luck [codex F1, A4]; UPDATE both; COMMIT —
  REPEATABLE READ), `scripts/drill_race.py` (barrier start, W workers × K
  transfers over hot accounts incl. paired opposing withdrawals sized to
  race overdraft, journal, end-of-run conservation report).
  Verify: `for i in 1 2 3; do make drill-race MODE=naive; done` → every run
  reports SUM(balance) ≠ seeded total and/or negative balance, non-zero
  lost-update count.

- [x] **T5 — Strategy a: pessimistic** [D1]
  Files: `scripts/strategies.py` (`transfer_pessimistic`: FOR UPDATE in
  sorted-id order, 1213-retry wrapper with cap).
  Verify: `make drill-race MODE=a` → conserved, no negatives, rejects
  logged with reason `insufficient`.

- [x] **T6 — Strategy b: optimistic** [D1]
  Files: `scripts/strategies.py` (`transfer_optimistic`: read w/o lock,
  conditional UPDATE on `version`, bounded retry; retries counted).
  Verify: `make drill-race MODE=b` → conserved; journal shows retries > 0
  under contention.

- [x] **T7 — Strategy c: atomic conditional** [D1]
  Files: `scripts/strategies.py` (`transfer_atomic`: single
  `UPDATE … SET balance = balance - x WHERE id=? AND balance >= x`, then
  credit; both inside one txn).
  Verify: `make drill-race MODE=c` → conserved, zero negatives, rejects on
  affected-rows==0.

- [x] **T8 — Strategy d: ledger** [D1, A3]
  Files: `scripts/strategies.py` (`transfer_ledger`: append debit/credit
  entries with per-account seq as serialization point, update
  `balance_cache`; overdraft check against cache under its row lock).
  Verify: `make drill-race MODE=d` → conserved AND per-account
  `balance_cache == SUM(entries)`.

- [x] **T9 — Deadlock drill** [D3]
  Files: `scripts/drill_deadlock.py` (A→B vs B→A concurrently, ORDER=naive
  → count 1213s; ORDER=sorted → assert zero).
  Verify: `make drill-deadlock` prints deadlocks>0 (naive) then ==0 (sorted).

- [x] **T10 — verify.py** [A3]
  Files: `scripts/verify.py` (conservation vs seed.json, no negatives,
  journal acked==applied exactly once vs `transfers` table, mode-d cache
  reconciliation; exit 1 on any violation), Makefile `verify` +
  `verify-naive` (expected exit 1) targets.
  Verify: after T5–T8 runs `make verify` → 0; `make verify-naive` → 1.

- [x] **T11 — bench.py** [D4, A5]
  Files: `scripts/bench.py` (reset+reseed per mode, identical contended
  workload a/b/c/d, table: ops/s, p50, p95, retries, deadlocks, conserved
  Y/N; OPS env-tunable).
  Verify: `make bench` prints the 4-row table in < ~2 min.

- [x] **T12 — demo target** [AC1]
  Files: `Makefile` (`demo:` chain — up install bootstrap seed → naive race
  → verify-naive (expected exit 1, guarded) → **reseed** → a/b/c/d races
  (clean world, so conservation claims are valid [codex F4]) → deadlock
  drill → verify → bench, numbered `===` banners).
  Verify: `make clean && make demo` full pass ≤ ~5 min.

- [x] **T13 — README** [AC8, D5]
  Files: `README.md` (7 sections; §3.5 road-not-taken = SERIALIZABLE [D2];
  §6 interview questions; §7 file map; runbook outputs from a real run).
  Verify: every runbook command exists in Makefile; section headers match
  convention.

- [x] **T14 — Root bookkeeping** [AC8]
  Files: `/README.md` (lab 11 row), `/BACKLOG.md` (P04 → ✅ Covered,
  Notes → Lab 11).
  Verify: `git diff` shows exactly those two edits.

- [x] **T15 — End-to-end check (spec E2E)**
  From pristine (`make clean`): `make demo`, then `make verify` exit 0,
  `make clean` leaves no `.jsonl/.pid/.log/volumes`.
  Verify: the commands above, output quoted in log.md.

Note on TDD shape: this repo's tests are the drills themselves (no pytest —
lab 10 precedent). "Red" for each strategy task = the naive mode's violation
(T4) is the standing failing state; each strategy turns the same drill green.
verify.py's red is `verify-naive` exit 1.

- [x] **T16 — done-gate fixes** [gate: FIX FIRST]
  Files: `scripts/drill_race.py` (barrier timeouts + dead-worker tolerance,
  journal append not unlink, read→write rendezvous for naive, zero-acked
  guard + error accounting, HOT/ACCOUNTS validation),
  `scripts/strategies.py` (sync hook in naive; deadlock counters in a/b/c/d),
  `scripts/drill_deadlock.py` (stats lock, balance restore, barrier timeout),
  `scripts/bench.py` (deadlocks column), `README.md` (refresh sample table).
  Rejected on record: D1 one-script-each, A1 schema-level wording, make -j,
  bench subprocess overhead, BACKLOG prose (user to confirm).
  Verify: repeat-run verify, standalone deadlock+verify, naive 3x, full demo.
