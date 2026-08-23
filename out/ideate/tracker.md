# Ideate tracker

## Run 2026-08-15 (first run)

**Frame**
- Job: teach senior/staff-level data-infra operations (live migrations, consistency, failover, zero-downtime) via fully runnable, deterministic, break-it-yourself local labs.
- User: mid-to-senior backend engineers leveling to staff scope (+ interview preppers).
- Differentiation axis: "not a toy demo" — deterministic reproduction of real production disasters, with failure drills and rollback practice.
- State at run: 7/9 labs ready; 08 & 09 PLAN.md only; no CI; commit 9e5acaf.

**Verdicts**

| Idea | Lens(es) | Verdict | Reason / trigger / handoff |
|---|---|---|---|
| Finish labs 08 (idempotent events) + 09 (expand/contract capstone) | baseline | EXPLORE | Completes the promised series; PLANs exist. → `/feature` from 08's first unchecked milestone. |
| CI anti-bit-rot harness + universal `make verify` oracle | form-factor, failure-mining, compress | EXPLORE | Defends the axis: a lab that no longer runs is a toy demo. → `/feature`. |
| Chaos/exam mode (`make chaos`: hidden fault, scored diagnosis, rubric) | inversion, steal (check rides/CTF), failure-mining | EXPLORE | Makes "break it on purpose" twice as true. → `/priorart` first (toxiproxy/pumba/chaos tooling reuse vs build). |
| Postmortem citations + per-lab determinism contract | diff-deepening | PARK | Cheap credibility doc work. Revive when: labs 08/09 ship. |
| Compound-failure capstone (failover during resharding cutover; lab 10) | combine | PARK | Revive when: series (01–09) complete. |
| `labctl` guided runner (auto-checked runbook steps, spaced drills) | form-factor, compress, steal (spaced rep) | PARK | Revive when: evidence learners stumble on runbooks, or CI step-assertions exist to reuse. |
| Zero-install: devcontainer/Codespaces + asciinema walkthroughs | removed-constraint, adjacent-user | PARK | Revive when: repo goes public / first external user. |
| Scale-knob benchmarks (10M-row seed, published timings) | 10x | PARK | Revive when: CI harness lands. |
| Team game-day / interviewer packaging (facilitator guides, answer keys) | adjacent-user | PARK | Revive when: an external team or interviewer asks. |
| Book / course publication | form-factor | PARK | Revive when: 9/9 labs ready + CI green. |
| AI-agent eval benchmark (can an agent run the migration?) | combine | DROP | Off-axis: product teaches humans, not benchmarks agents. |
| Stack variants (Postgres, Redpanda, …) | removed-constraint | DROP | Maintenance multiplier; breadth over the depth that is the moat. |
| Persistent shared "company system" across labs | removed-constraint | DROP | Breaks self-containment convention; no survivable user-impact line. |

## Run 2026-08-15b (scoped: new-lab topics for labs 10+)

**Frame**
- Same job/user/axis as run 2026-08-15. Scope narrowed to lab *content* ideas only (meta ideas verdicted last run stand).
- Gap analysis of curriculum: transactions/isolation, overload dynamics, data-corruption recovery, query-level cascades, multi-region, queue ops are uncovered.

**Verdicts**

| Idea | Lens(es) | Verdict | Reason / trigger / handoff |
|---|---|---|---|
| Lab 10: transaction isolation anomalies (lost update, write skew, phantoms; deterministic per isolation level) | diff-deepening | EXPLORE | Most on-axis gap; interview gold. → `/priorart` (Kleppmann's hermitage) then `/feature`. |
| Lab 11: retry storm / metastable failure (blip + naive retries = permanent overload; jitter, budgets, breakers, shedding) | steal (SRE) | EXPLORE | The dominant modern outage shape, reproducible locally. → `/feature`. |
| Lab 12: bad deploy corrupted data — PITR + surgical binlog replay (drill: backup is corrupt) | inversion | EXPLORE | Data-disaster recovery is the scariest unpracticed skill; absorbs "backup verification drill". → `/feature`. |
| One bad query kills everything (MDL pileup; pool-exhaustion cascade) | failure-mining | PARK | Strong; revive after the three EXPLOREs ship (natural lab 13). |
| Hot key / hot shard after reshard (salting, coalescing) | 10x | PARK | Revive when lab 06 gets usage feedback. |
| Multi-region active-active LWW conflict loss + clock skew | removed-constraint, steal | PARK | Heavier infra; revive when 01–09 complete. |
| Kafka rebalance storm / consumer-lag death spiral | form-factor | PARK | Revive after lab 08 ships (reuse its Kafka stack). |
| Saga / partial failure across services | removed-constraint | PARK | Possible overlap with 08 outbox; revisit once 08 built. |
| Event schema evolution / registry compatibility | compress | PARK | May be covered by 09 compatibility windows; check after 09. |
| CDC while source reshards (03×06) | combine | DROP | Folded into already-PARKed compound capstone. |
| ES cluster ops (hot node, shard imbalance) | form-factor | DROP | Tuning-shaped, not deterministic-disaster-shaped. |
| Incident-mystery labs (diagnose unknown fault) | adjacent-user | DROP | Duplicate of EXPLOREd chaos/exam mode, not new lab content. |
