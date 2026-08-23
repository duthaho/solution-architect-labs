# Lab 13 — Distributed unique ID generation (spec)

Backlog item **P05b**: short, sortable IDs, no collisions, at tens of
thousands of IDs/s across multiple workers — Snowflake-style, with the
failure drills that make it a lab instead of a snippet.

## Goal (observable behavior)

A new self-contained lab at `labs/13-unique-id-generation/` following repo
conventions (docker-compose, Makefile, README, pinned requirements). `make
demo` tells the whole story end-to-end and **exits 0 only if every gate
passes**; `make clean` returns the machine to pristine.

The story `make demo` tells:

1. **The bug, reproduced**: a naive generator (current timestamp + worker id
   + wrapping sequence, no guards) emits **duplicate IDs deterministically**
   under two injected failures — a backwards clock jump and sequence
   exhaustion within one millisecond. The verifier catches it (inverted-exit
   proof, same pattern as labs 11/12).
2. **The fixes, compared**: hardened policies side by side in drills.
3. **The alternatives, measured**: Snowflake vs UUIDv4 vs UUIDv7 vs MySQL
   auto-increment vs Redis INCR — generation bench + a B-tree insert-locality
   bench showing *why* sortable keys matter.
4. **The coordination story**: worker IDs are leased from MySQL with TTL +
   heartbeat; a zombie-worker drill proves ID reuse is safe.

## Decisions

- **D1** — Lab 13 builds backlog **P05b** (user choice over P05/P14/P15).
- **D2** — ID layout: 64-bit Snowflake — 41 bits milliseconds since a custom
  epoch, 10 bits worker id, 12 bits sequence. Fits in a signed BIGINT;
  k-sortable by construction.
- **D3** — **Worker-ID assignment via MySQL lease**: a `worker_leases` table;
  a worker claims a free id row with an owner token and TTL, heartbeats to
  renew, and must **check lease ownership before emitting** (cheaply, via a
  local validity window — not a DB round-trip per ID). Zombie drill:
  SIGSTOP a worker past its TTL, let another claim the same worker id, then
  resume the zombie — it must detect the lost lease and stop/re-acquire, and
  the verifier must show zero duplicate IDs across the handoff.
- **D4** — **Backwards-clock drill compares policies** side by side, with an
  injectable clock for determinism:
  - `naive` — trust the clock → duplicates (the reproduced bug)
  - `error` — refuse to generate while `now < last_ts` (fail fast)
  - `wait` — sleep out small regressions (bounded), error beyond the bound
  - `hold` — keep generating at `last_ts` by consuming remaining sequence,
    spilling forward only when sequence exhausts
- **D5** — **Sequence-exhaustion drill**: frozen clock forces >4096 IDs in
  one logical ms; `naive` wraps and duplicates, hardened generator spins to
  the next ms. Exit-code asserted.
- **D6** — **Comparison set**: Snowflake (this lab), UUIDv4, UUIDv7 (Python
  implementation pinned), MySQL `AUTO_INCREMENT`, Redis `INCR`. Measured on:
  generation throughput (multi-process), ID size (bits / text length),
  sortedness of the merged stream (fraction of adjacent inversions),
  coordination required (qualitative row in README).
- **D7** — **B-tree insert-locality bench**: insert the same N rows into
  MySQL tables keyed by `BINARY(16)` UUIDv4 vs UUIDv7 vs `BIGINT` Snowflake;
  report insert throughput and index size (`information_schema` /
  `SHOW TABLE STATUS`). This is the evidence for "random PKs fragment the
  B-tree".
- **D8** — `verify.py` invariant gate joins worker journals and asserts:
  global uniqueness, per-worker strict monotonicity, layout round-trip
  (decode(id) matches journal's worker/ts claims), and lease invariants
  (no two acked IDs share worker-id from overlapping lease windows of
  different owners). `VERIFY_INVERT=1` must pass after the naive runs.

## Assumptions

- **A1** — Generators are Python multi-process workers (no HTTP/gRPC
  service), consistent with labs 11/12; contention and parallelism come from
  `multiprocessing`, synchronized with barriers for determinism.
- **A2** — Clock faults are **injected via an injectable clock** (a
  controllable time source shared per drill), not real NTP skew — that's what
  makes the drills deterministic on any laptop.
- **A3** — Ports follow the series: MySQL `:3319`, Redis `:6391`, optional
  adminer `:8093` behind the `ui` profile. MySQL 8.0.x, Redis 7.x pinned.
- **A4** — Lab directory name: `labs/13-unique-id-generation`.
- **A5** — Redis is used **only** as an INCR comparison target (D6), not for
  leases (user chose MySQL lease, D3).

## Out of scope

- No networked ID service (no HTTP/gRPC endpoint, no client library).
- No multi-datacenter / datacenter-id bits, no ZooKeeper/etcd coordination.
- No UUIDv1/v6/ULID variants beyond the D6 set.
- No changes to existing labs.

## Acceptance criteria

1. `make demo` runs the full story (bug → gates catch it → fixes → drills →
   benches → verify) and exits 0; every drill also runs standalone per the
   README runbook.
2. Naive duplicates reproduce **every run** (drills are deterministic);
   `make verify-naive` (inverted) exits 0, proving the gate is non-vacuous.
3. Zombie-worker drill: lease expires under SIGSTOP, worker id is re-claimed,
   zombie detects loss on resume; verifier reports zero duplicates.
4. Benches print comparison tables (generation + B-tree) that run to
   completion on a laptop in ≲2 minutes each.
5. `make clean` leaves no containers, volumes, or generated files;
   repo-level `README.md` gains the lab 13 row and `BACKLOG.md` flips P05b
   to ✅ Covered with the mapping.
6. Lab README follows the series shape: problem → architecture diagram →
   deep dive → runbook with expected output → production checklist →
   interview questions → file map.

## Amendments (recorded during build)

- **D6 (sortedness metric)** — "fraction of adjacent inversions" replaced by
  **normalized rank displacement** (mean |arrival rank − sorted rank| / N).
  At ~170k uuid7/s thousands of ids share one millisecond, so adjacent pairs
  compare random bits and the uuid4 margin was luck (49.61% vs 50.01%);
  displacement measures the actual B-tree locality story and separates the
  schemes ~200×. Logged at T9.
- **A1 (determinism mechanism)** — determinism comes from **injectable
  clocks** (scripted/offset/file clocks), not barriers; barriers proved
  unnecessary once every drill's timestamps were scripted.
- **D6 (UUIDv7)** — vendored ~20-line RFC 9562 implementation in
  `alternatives.py` instead of pinning a third-party package: zero extra
  dependencies, and the bit layout is itself teaching material.

## End-to-end check

```bash
cd labs/13-unique-id-generation && make demo && make clean
```

Exit 0 on both.
