# Plan — Lab 10: Soft delete for many big tables

Repo template: lab 02 (`labs/02-mysql-online-migration/`). All paths below are
under `labs/10-soft-delete-big-tables/` unless prefixed with `/`.

Note on TDD: this repo's labs have no pytest suites by convention — the runnable
proof per task is the script/make target itself, and `verify.py` is the lab's
test. Each task below carries that verification command.

**Isolation model (fixes codex finding "cross-strategy contamination"):** each
strategy gets its own identically-seeded schema family from the same RNG seed:
`lab10_a` (deleted_at), `lab10_b` + `lab10_b_deleted` (mirror), `lab10_c` +
`lab10_c_archive` (archiver). Traffic and journals are per-strategy
(`traffic_a.jsonl`, …). Strategies never share state; `bench` compares like
against like.

**Journal contract (fixes "consumer contract"):** `traffic_<s>.jsonl` lines are
`{"op":"insert|update|delete_request","table":...,"id":...,"ts_ms":...}`, acked
ops only. Each strategy script consumes `delete_request` rows and appends
outcomes to `outcome_<s>.jsonl`: `{"id","table","action"}` where action ∈
`soft_deleted|moved|archived|purged|restored`. `verify.py` joins journal +
outcomes + DB state.

## Tasks

- [x] **T1 — Scaffold infra** [D1]
      Files: `docker-compose.yml`, `requirements.txt`, `Makefile` (targets:
      `help install up down clean`).
      Lab-02 patterns: `mysql:8.0.43`, container `lab10-mysql`, port 3316,
      named volume, `mysqladmin ping` healthcheck, `PyMySQL==1.1.1`.
      Verify: `make install && make up` → healthy; `make down`.

- [x] **T2 — Schema + bootstrap** [A1, D2]
      Files: `sql/schema.sql`, `scripts/bootstrap.py`, `scripts/common.py`.
      Three schema families per isolation model. Every **live** table in a and c
      carries `deleted_at DATETIME NULL` + secondary index on it (codex: A and C
      need it on live tables; b does not — delete means move). `users.email`
      UNIQUE in every family. FKs: `orders.user_id → users.id`,
      `order_items.order_id → orders.id` on live tables only; **mirror and
      archive tables keep the same PKs but no FKs** (deliberate, discussed in
      README). Mirror tables add trailing `_deleted_at, _deleted_by` columns.
      `lab10_c_archive` PK = source PK (idempotent re-copy target).
      `common.py`: env config, `connect()`, per-family helpers, seeded row
      factories, p50/p95 timing helper.
      Verify: `make bootstrap` idempotent (run twice); table lists match across
      families.

- [x] **T3 — Seed** [A2]
      Files: `scripts/seed.py`, Makefile `seed` (`SEED_ROWS ?= 500000`).
      Seeds all three families from the same seed → identical content. Batched
      multi-value INSERTs, users:orders:items ≈ 1:5:15, progress every 100k.
      Verify: `make seed SEED_ROWS=20000` → per-family counts identical and
      match ratios.

- [x] **T4 — Traffic generator** [A5]
      Files: `scripts/traffic.py` (`--family a|b|c`), Makefile
      `traffic-start/traffic-stop` (nohup + .pid per family).
      Journal contract above; SIGTERM graceful; retry 1205/1213/2003/2006/2013
      with backoff. `delete_request` = row id appended to journal only (the
      strategy consumes it); inserts/updates applied directly to the family's
      live tables.
      Verify: 10s run against family a; journal valid JSON lines, acked only.

- [x] **T5 — Strategy A: deleted_at** [D2-A]
      Files: `scripts/strategy_a.py`, Makefile `strategy-a`.
      Consumes delete_requests → `UPDATE ... SET deleted_at=NOW()` (cascade:
      user → their orders → items), writes `outcome_a.jsonl`. Then deterministic
      drills in-script: (a) forgotten-WHERE — revenue report with and without
      the filter, prints the wrong vs right totals; (b) resurrection — soft
      delete a user, INSERT same email → catches ER_DUP_ENTRY, prints it, then
      shows the workaround discussion pointer. Measures read p50/p95 filtered
      vs unfiltered + `information_schema` table/index bytes after soft-deleting
      30% of rows.
      Verify: `make strategy-a` exits 0; output contains both reproduced
      failures + timing/size table; `outcome_a.jsonl` covers all delete_requests.

- [x] **T6 — Strategy B: mirror deleted-schema** [D2-B, A3]
      Files: `scripts/strategy_b.py`, Makefile `strategy-b`, `drill-drift`.
      Move in ONE transaction with explicit ordering (codex): mirror inserts
      parent-first (users → orders → items), live deletes child-first (items →
      orders → users). Insert uses positional `INSERT INTO mirror_t SELECT t.*,
      NOW(), 'user_req' FROM t ...` — deliberately positional, because that is
      exactly what breaks under drift. Restore = reverse (live inserts
      parent-first, mirror deletes child-first). Drill-drift: `ALTER TABLE
      lab10_b.orders ADD COLUMN coupon VARCHAR(32) NULL` (mirror untouched) →
      next move fails with column-count error (quoted) → repair = apply same
      ALTER to mirror → move succeeds.
      Verify: `make strategy-b` — per-table moved counts match, restore
      round-trips one user byte-identical (checksum compare); `make drill-drift`
      reproduces the exact error then recovers.

- [x] **T7 — Strategy C: archiver + purge** [D2-C]
      Files: `scripts/archiver.py`, Makefile `archiver-start/archiver-stop`,
      `drill-kill-archiver`, `drill-big-delete`.
      Soft delete is instant (`deleted_at=NOW()` via strategy script reusing A's
      consumer against family c). Archiver loop: one short transaction per
      batch = `INSERT ... ON DUPLICATE KEY UPDATE` copy of ≤`BATCH ?= 500` rows
      `WHERE deleted_at IS NOT NULL` + `DELETE` same ids, commit, sleep.
      **No checkpoint table needed** (codex): the predicate is the work queue —
      restart re-selects; copy+delete atomicity in one txn + idempotent copy
      target PK ⇒ kill -9 anywhere loses/dupes nothing. Retention purge target
      for archive rows older than `RETENTION_DAYS`.
      Drill-big-delete (codex: objective criteria): a reader thread samples a
      point-read every 100ms into arrays; run giant single `DELETE` (30% rows)
      vs batched equivalent on re-seeded state; print reader p95 during each
      and **assert batched p95 < giant p95**, non-zero exit otherwise.
      Verify: `make drill-kill-archiver` → kill -9 mid-run, restart, live+archive
      counts exactly partition the ids; `make drill-big-delete` prints contrast
      and passes its assertion.

- [x] **T8 — verify.py: exactly-once invariant** [A5]
      Files: `scripts/verify.py`, Makefile `verify`.
      Per family: join `traffic_<s>.jsonl` + `outcome_<s>.jsonl` + DB. Invariant:
      every journaled id exists in exactly one of live/mirror/archive (family-
      appropriate), every delete_request has an outcome, no soft-deleted row
      appears in the filtered read. Non-zero exit + sample of violating ids.
      Verify: passes after T5–T7; then manually re-insert one archived row into
      live → `make verify` exits non-zero naming that id.

- [x] **T9 — bench.py comparison table** [A4]
      Files: `scripts/bench.py`, Makefile `bench`.
      On identically re-seeded families, same delete workload under traffic per
      family: collect delete-op p50/p95, read p95 (correct filtered query),
      live table+index bytes after, restore support. Prints final ASCII
      comparison table.
      Verify: `make bench SEED_ROWS=20000` prints table, all cells filled.

- [x] **T10 — `make demo` end-to-end** [AC1]
      Files: `Makefile` `demo`: up → bootstrap → seed → per-family
      traffic + strategy → **drills included** (drift, kill-archiver,
      big-delete — codex: demo must assert what it claims) → bench → verify →
      stop. `make clean` = compose down -v + rm state files.
      Verify: from clean, `make demo SEED_ROWS=50000` completes green; `make
      clean` pristine.

- [x] **T11 — Lab README.md** [AC5, D2, D3]
      Files: `README.md`, 7-section structure: §1 problem, §2 architecture,
      §3 deep dives (3.1 why soft delete, 3.2 A: unique/index pain + Postgres
      partial-index comparison, 3.3 B: consistency + drift tax + why mirrors
      drop FKs, 3.4 C: the production pattern, 3.5 partitioning as the road not
      taken), §4 runbook + Drills 1–5 with real pasted output, §5 production
      checklist (100M rows), §6 8–10 interview questions, §7 file map. No ORM
      content (D3).
      Verify: headings match repo structure; every §4 command actually run.

- [x] **T12 — Root README row** [D4]
      Files: `/README.md` — add row 10 to labs table.
      Verify: link resolves.

- [x] **T13 — E2E acceptance** [AC1–AC6]
      Fresh clean → default `make demo` (500k, timed) → `verify: OK` → clean →
      `git status` shows only intended files.
