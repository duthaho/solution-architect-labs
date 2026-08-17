# Lab 09 — Deploy with no data gap and no downtime (expand/contract)

> **Status: IMPLEMENTED.** See [README.md](README.md). This file keeps only
> the design deltas discovered during the build.

All six milestones landed as planned (compose + nginx traffic switch, app with
boot-time version switch, rolling + blue-green deploy machinery, naive drill,
expand/migrate/contract ladder, four failure drills, `make demo` comparison).

Deltas vs the original plan, all documented in the README deep-dives:

- **Four app versions, not three.** The plan's "v2 = new columns only"
  conflated two steps that cannot ship together: flipping reads (v2: read new,
  still dual-write) and abandoning old writes (v3). Collapsing them makes the
  read-flip irreversible — v1.5 could no longer roll back cleanly. README §3.1.
- **A `relax` rung exists (contract step 0).** v3 can't deploy while `name` is
  `NOT NULL` without a default: every v3 INSERT dies with error 1364. Found
  empirically (12s window of 500s), kept as a teaching point. README §3.2.
- **The backfill predicate is "missing OR disagreeing", not `IS NULL`.** The
  v1→v1.5 mixed window can leave populated-but-stale first/last (v1.5
  dual-write then v1 name-only write on the same row); the shapes verifier
  caught one such row live. README §3.3.
- The naive drill's migration is add + copy + DROP (not just ALTER+deploy),
  so it demonstrates both failure classes: visible 500s AND silently lost
  acked writes.
