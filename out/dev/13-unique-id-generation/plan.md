# Lab 13 — plan

Repo has no pytest harness; **drills are the tests** (exit-code contract,
lab-11/12 style). "Red" per task = run the drill/selftest before the code it
exercises exists or while the bug is unfixed, see it fail for the right
reason; "green" = the documented exit code. All paths relative to
`labs/13-unique-id-generation/` unless noted.

- [ ] **T1 — Scaffold** [A3, A4]: `docker-compose.yml` (mysql 8.0.43 `:3319`
  as `lab13-mysql`, redis 7.4.1 `:6391` as `lab13-redis`, adminer `:8093`
  profile `ui`), `.gitignore`, `requirements.txt` (PyMySQL + redis, pinned
  same as lab 12), `Makefile` with `help/up/down/clean/install` targets;
  compose healthchecks + `up` uses `docker compose up -d --wait` (lab-12
  pattern). *Verify:* `make up install` exits 0 only when both healthy.
- [ ] **T2 — Schema + common** [D3, A3]: `sql/schema.sql` (`worker_leases`
  (worker_id PK, owner, expires_at) **pre-seeded rows 0..15 by bootstrap**
  so claim always has rows to contend for, `seq_autoinc` for the D6 comparison),
  `scripts/common.py` (env config, connect, jsonl journals, percentiles —
  lab-12 pattern), `scripts/bootstrap.py`. Add `bootstrap` target.
  *Verify:* `make bootstrap` exit 0; `SHOW TABLES` lists both.
- [ ] **T3 — Snowflake core** [D2, D4, D5, A2]: `scripts/snowflake.py` —
  41/10/12 encode/decode, injectable clock, `naive` generator (trusts clock,
  wrapping sequence) and hardened generator with policies `error|wait|hold` +
  spin-to-next-ms. `--selftest` asserts encode/decode round-trip, k-ordering,
  and that selftest FAILS if bits overlap (temporarily broken constant to
  prove it, then fixed). Add `selftest` target. *Verify:* red = round-trip
  assert fails before encode/decode complete; green = `make selftest` exit 0.
- [ ] **T4 — Lease manager** [D3]: `scripts/lease.py` — claim lowest free
  worker-id row (single txn, owner token), heartbeat renew, local validity
  window (`safe_until = expires_at - margin`, no per-ID round-trip),
  `LeasedGenerator` that refuses to emit past `safe_until` and re-checks
  ownership; **every claim/renew/lost event appended to
  `lease_events.jsonl`** (verify's input). *Verify:* inline demo
  `python scripts/lease.py`: two claimants get different ids; expired lease
  is reclaimable; exit 0.
- [ ] **T5 — Zombie drill (riskiest)** [D3, A2, acceptance 3]:
  `scripts/drill_zombie.py` — multiprocess: worker A claims id, journals IDs
  to `ids_zombie_a.jsonl`, SIGSTOP past TTL; B claims the same worker id,
  journals to `ids_zombie_b.jsonl`; SIGCONT A → A must detect the lost lease
  (owner token mismatch / past `safe_until`) and stop. **Both workers draw
  timestamps from the same scripted logical clock (file-based step sequence),
  so in `ZOMBIE_NAIVE=1` mode resumed-A and B traverse identical (ts, seq)
  pairs → duplicates are guaranteed, not incidental.** Exit 0 iff the id was
  reused AND zero duplicate IDs across journals AND A's post-resume emission
  count is 0; naive mode inverts (exit 0 iff duplicates reproduced). Add
  `drill-zombie` / `drill-zombie-naive` targets. *Verify:* both targets exit
  0, three runs in a row.
- [ ] **T6 — Clock drill** [D4, A2, acceptance 2]: `scripts/drill_clock.py` —
  clock = real time + scripted offset; the injected backwards jump is
  **smaller than the `wait` policy's bound (`WAIT_MAX_MS`), so `wait`
  provably terminates** (real time keeps flowing under the offset); run
  `naive|error|wait|hold` side by side, journal each to
  `ids_clock_<policy>.jsonl`, print a table (dupes, errors raised, waited
  ms, IDs emitted; expected counts stated per policy). Exit 0 iff naive
  produced ≥1 duplicate AND `error` raised ≥1 AND `wait`/`hold` produced
  their expected counts with 0 dupes. Add `drill-clock` target. *Verify:*
  `make drill-clock` exit 0, naive dupes every run.
- [ ] **T7 — Exhaustion drill** [D5, A2]: `scripts/drill_exhaustion.py` —
  scripted clock **frozen for the first 5000 draws, then auto-advances 1 ms**
  (so hardened spin terminates deterministically); request >4096 IDs in one
  logical ms: naive wraps (duplicates), hardened spins to next ms (no dupes,
  correct count). Journals `ids_exhaustion_{naive,hardened}.jsonl`. Exit 0
  iff both observed. Add `drill-exhaustion` target. *Verify:* exit 0,
  deterministic, terminates < 30 s.
- [ ] **T8 — Invariant gate** [D8]: `scripts/verify.py` — inputs are explicit:
  all `ids_*.jsonl` **except** `ids_*naive*.jsonl`, plus `lease_events.jsonl`.
  Asserts: global uniqueness, per-worker strict monotonicity, decode(id)
  matches journaled worker/ts, no two owners' lease windows overlap for the
  same worker id. `VERIFY_INVERT=1` checks **only** the `ids_*naive*.jsonl`
  journals and exits 0 iff they violate. Targets `verify` / `verify-naive`.
  *Verify:* after T5–T7 → `make verify` exit 0 AND `make verify-naive` exit 0
  (naive journals exist and are caught).
- [ ] **T9 — Alternatives + generation bench** [D6]: `scripts/alternatives.py`
  (uuid4, RFC-9562 UUIDv7 impl ~20 lines, MySQL `AUTO_INCREMENT` insert,
  Redis `INCR`), `scripts/bench.py` — WORKERS processes × N ids per scheme:
  ops/s, p50/p95, bits/text len, merged-stream adjacent-inversion fraction.
  Add `bench` target. **Exit-code gates only correctness-shaped facts**
  (uniqueness within the bench run, sortedness ordering
  uuid7/snowflake < uuid4 inversion fraction — structural, not timing);
  throughput numbers are informational, narrated in the README. *Verify:*
  `make bench` completes ≲2 min, table printed.
- [ ] **T10 — B-tree bench** [D7]: `scripts/bench_btree.py` — fixed dataset
  (same N rows, same payload, single-column PK difference only): `BINARY(16)`
  uuid4 / uuid7, `BIGINT` snowflake; report rows/s + index size
  (`information_schema.innodb_tablespaces` or `SHOW TABLE STATUS`).
  **Exit 0 = ran to completion and printed the table** — relative speed is
  environment-sensitive and is narrated, not gated. Add `bench-btree`
  target. *Verify:* ≲2 min, table printed.
- [ ] **T11 — `make demo` wiring** [acceptance 1]: full story: up→install→
  bootstrap→selftest→drill-clock→drill-exhaustion (**these leave the
  `ids_*naive*.jsonl` journals in place**)→verify-naive→drill-zombie-naive→
  drill-zombie→verify→bench→bench-btree, numbered `=== N. ===` echoes
  (lab-12 style). *Verify:* fresh `make clean && make demo` exit 0.
- [ ] **T12 — Lab README** [acceptance 6]: `README.md` in series shape:
  problem → architecture ASCII → deep dive (bit layout, lease safety
  argument, clock policies, why sortable PKs) → runbook with real captured
  output/bench numbers → production checklist → 10 interview questions →
  file map. *Verify:* every runbook command exists in the Makefile.
- [ ] **T13 — Repo docs** [acceptance 5]: repo `README.md` lab-13 row;
  `BACKLOG.md` P05b → ✅ Covered with mapping + update "what to build next".
  *Verify:* git diff shows only those edits.
- [ ] **T14 — End-to-end check** [spec E2E]:
  `cd labs/13-unique-id-generation && make demo && make clean`, both exit 0;
  `docker ps` empty of lab13, no stray artifacts.
