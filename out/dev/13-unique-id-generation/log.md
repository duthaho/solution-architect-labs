# Lab 13 — log

- 2026-08-20 · spec approved (P05b chosen; MySQL lease, policy-comparison clock drill, B-tree bench all accepted as recommended)
- 2026-08-20 · plan approved after codex cross-model review; 8 findings folded in (deterministic zombie duplicate via shared scripted clock, frozen-clock auto-advance, pre-seeded lease rows, benches not gated on timing, explicit verify inputs)
- 2026-08-20 · T1 scaffold · green (containers healthy) · 9a143dc
- 2026-08-20 · T2 schema/common/bootstrap · green after fixing `;` inside SQL comments breaking the statement splitter · 752bc46
- 2026-08-20 · T3 snowflake core · green; SELFTEST_BREAK=1 proves the selftest catches a broken layout · 1eca496
- 2026-08-20 · security scan flagged compose (throwaway creds, exposed ports, adminer) — accepted by design: series-wide local-lab convention, identical to labs 01–12, adminer opt-in via `ui` profile
- 2026-08-20 · T4 lease manager · green (distinct claims, expiry reclaim, refusal past safe_until, heartbeat-lost)
- 2026-08-20 · T5 zombie drill · green 3x both modes (naive: exactly 50 dupes via shared scripted clock) · 4579ff3
- 2026-08-20 · T6+T7 clock/exhaustion drills · green 3x, exact counts (3 / 904 dupes) · 7efdd22
- 2026-08-20 · T8 invariant gate · green; inverted mode catches all 957 injected violations · 14cc4fb
- 2026-08-20 · T9 bench · adjacent-inversion metric replaced with rank displacement (uuid7 margin was 0.4pp luck; displacement separates ~200x) · 17678aa
- 2026-08-20 · T10 btree bench · green (snowflake ~1.7x rows/s, 60% index size vs uuid4) · a443e4a
- 2026-08-20 · T11 demo chain · green, exit 0 · (commit "demo chain")
- 2026-08-20 · T12 README, T13 repo registration · 3bc9dd1, f2810ae
- 2026-08-20 · T14 E2E from pristine: demo=0 clean=0, zero containers/artifacts left
