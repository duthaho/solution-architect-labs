# Lab 13 — log

- 2026-08-20 · spec approved (P05b chosen; MySQL lease, policy-comparison clock drill, B-tree bench all accepted as recommended)
- 2026-08-20 · plan approved after codex cross-model review; 8 findings folded in (deterministic zombie duplicate via shared scripted clock, frozen-clock auto-advance, pre-seeded lease rows, benches not gated on timing, explicit verify inputs)
- 2026-08-20 · T1 scaffold · green (containers healthy) · 9a143dc
- 2026-08-20 · T2 schema/common/bootstrap · green after fixing `;` inside SQL comments breaking the statement splitter · 752bc46
- 2026-08-20 · T3 snowflake core · green; SELFTEST_BREAK=1 proves the selftest catches a broken layout · 1eca496
- 2026-08-20 · security scan flagged compose (throwaway creds, exposed ports, adminer) — accepted by design: series-wide local-lab convention, identical to labs 01–12, adminer opt-in via `ui` profile
- 2026-08-20 · T4 lease manager · green (distinct claims, expiry reclaim, refusal past safe_until, heartbeat-lost)
