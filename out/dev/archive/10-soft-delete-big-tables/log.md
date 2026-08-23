# Log — lab 10 soft delete

- 2026-08-18 · spec approved (MySQL 8, strategies A/B/C runnable, no ORM) · —
- 2026-08-18 · plan approved after codex review (8 findings folded in: per-family isolation, journal contract, FK ordering, idempotent archiver w/o checkpoint, objective big-delete assert, drills in demo) · —
- 2026-08-18 · T1 scaffold (compose/Makefile/deps), verified healthy · 3dd5473
- 2026-08-18 · T2 schemas ×5 + bootstrap idempotent; fix: strip `--` comments before split (`;` in comment broke exec) · lab10: three schema families
- 2026-08-18 · T3 identical seed 1:5:15 across families · lab10: identical seed
- 2026-08-18 · T4 traffic w/ delete_request journal contract · lab10: per-family traffic
- 2026-08-18 · T5 strategy A; fix Decimal/float; drills A1 (44.4% overcount) + A2 (ER 1062) reproduce · lab10: strategy A
- 2026-08-18 · T6 strategy B; FOR UPDATE id-snapshot move; round-trip checksum OK; drift drill 1136→repair · lab10: strategy B
- 2026-08-18 · T7 archiver + purge; fixes: %% w/o params → MOD(); RR-snapshot stale reads in drill (→autocommit); drill work-supply made self-provisioning; reader probes deleted-set FOR SHARE for deterministic contrast (203ms vs 3ms at 50k) · lab10: strategy C
- 2026-08-18 · T8 verify; fix: cascade-moved children have no own outcome → XOR rule for b; corruption test catches planted dup id=77 · lab10: verifier
- 2026-08-18 · T9 bench table (resets world; runs per-family traffic during workload) · lab10: benchmark
- 2026-08-18 · T10 demo chain green at 50k from clean; clean pristine · lab10: make demo
- 2026-08-18 · T11+T12 README deep-dive w/ real outputs; root table row 10 · lab10: README
- 2026-08-18 · T13 full 500k demo running in background
- 2026-08-18 · T13 PASSED: 500k demo exit 0 in 14m19s; drills A1/A2/B1/C1/C2 all reproduce; verify 0 violations (10,749 rows); clean pristine · —
- 2026-08-18 · done gate: FIX FIRST — 9 accepted findings (S1 trigger discussion, S2 bench unfiltered+restore probes, S3 A2 executed recovery, C1 traffic 1452 skip, C2 kill-drill vacuous-pass guard, C3 strategy deadlock retry, C4 torn jsonl line, C5 roundtrip race retry, C6 sample-count guard); rejected S4 (verify-before-bench order is deliberate: bench wipes journals); codex 2nd opinion unavailable (capacity) · —
- 2026-08-18 · re-gate: demo 50k exit 0 with all fixes proven in-run (recovery executed, kill mid-flight w/ 1838 flagged, restore probes OK×3, verify 0 violations) · 8067eb0
- 2026-08-18 · VERDICT: SHIP
