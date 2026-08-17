"""CONTRACT: drop `name` — the one step in the ladder you cannot roll back.

Because it's irreversible, it runs behind three independent proofs (any one
alone is circumstantial):

  1. Intent:  deploy state + every pod in the LB reports v3 (asked via
              /healthz, not trusted from the state file).
  2. At rest: no row is missing first/last (verify.py's null check).
  3. On the wire: a performance_schema digest soak — reset the statement-digest
     table, wait, then look for ANY statement touching `` `name` `` (exact
     backtick match, so `first_name` doesn't false-positive). This is the
     production trick: the schema can't tell you who still READS a column;
     only the query stream can. (Production: same scan over pt-query-digest
     or your query log pipeline, soaked for days, not seconds.)

--relax is contract's step ZERO, and it is load-bearing: `name` is NOT NULL
with no default, so code that stops writing it (v3) dies with error 1364 on
every INSERT while the column still exists. The constraint has to loosen
BEFORE the writers leave — this lab found that the hard way (the first v3
deploy produced a 12-second window of 500s until this step existed). Note the
cost asymmetry: MODIFY ... NULL rebuilds the table (online, but O(rows));
`ALTER COLUMN ... SET DEFAULT ''` would be INSTANT — the production trade-off
between "absent means NULL" and "absent means sentinel".

--force skips all of it — that is drill-early-contract, on purpose.
--restore is the undo-with-effort path: re-ADD name, refill it from
first/last. It only restores what dual-writing preserved — run it after
--force at v1.5 and nothing is lost; after the naive migration there is
nothing left to restore FROM.
"""
import argparse
import sys
import time

import common as c

DIGEST_TABLE = "performance_schema.events_statements_summary_by_digest"


def preconditions(soak_s: float) -> None:
    # 1: every pod taking traffic is v3
    state = c.read_deploy_state()
    for pod in c.in_lb():
        live = c.probe_pod(pod)
        assert live == "v3", f"{pod} is serving {live!r}, not v3 — contract would 500 it"
    c.log(f"precondition 1: all LB pods ({', '.join(c.in_lb())}) live-report v3 "
          f"(active color: {state['active_color']})")

    # 2: no row is missing the new shape
    with c.connect().cursor() as cur:
        cur.execute(f"SELECT COUNT(*) FROM {c.TABLE} WHERE first_name IS NULL")
        nulls = cur.fetchone()[0]
    assert nulls == 0, f"{nulls} rows still missing first/last — backfill incomplete"
    c.log("precondition 2: zero rows missing first/last")

    # 3: nobody on the wire touches `name` anymore
    with c.connect().cursor() as cur:
        cur.execute(f"TRUNCATE {DIGEST_TABLE}")
        c.log(f"precondition 3: digest soak — watching all statements for {soak_s}s...")
        time.sleep(soak_s)
        cur.execute(f"SELECT digest_text, count_star FROM {DIGEST_TABLE} "
                    "WHERE schema_name = %s AND digest_text LIKE %s", (c.DB, "%`name`%"))
        offenders = cur.fetchall()
    assert not offenders, ("statements still touching `name`:\n" +
                           "\n".join(f"  {n}x {t[:120]}" for t, n in offenders))
    c.log("precondition 3: soak clean — no statement referenced `name`")


def drop() -> None:
    t0 = time.time()
    with c.connect().cursor() as cur:
        cur.execute(f"ALTER TABLE {c.TABLE} DROP COLUMN name, ALGORITHM=INSTANT")
    c.log(f"CONTRACTED: DROP COLUMN name in {(time.time() - t0) * 1000:.0f}ms. "
          "There is no rollback from here — only --restore, which rebuilds "
          "what dual-writing preserved.")


def relax() -> None:
    with c.connect().cursor() as cur:
        cur.execute(f"SELECT is_nullable FROM information_schema.columns "
                    "WHERE table_schema=%s AND table_name=%s AND column_name='name'",
                    (c.DB, c.TABLE))
        row = cur.fetchone()
        assert row, "name column absent — nothing to relax"
        if row[0] == "YES":
            c.log("relax: name is already nullable — nothing to do")
            return
        t0 = time.time()
        cur.execute(f"ALTER TABLE {c.TABLE} MODIFY name VARCHAR(255) NULL")
    c.log(f"relax: name is now nullable ({time.time() - t0:.1f}s, online INPLACE "
          "rebuild — O(rows), unlike the INSTANT expand). v3 can deploy now.")


def restore() -> None:
    cols = c.columns()
    assert {"first_name", "last_name"} <= cols, "nothing to restore from"
    with c.connect().cursor() as cur:
        if "name" not in cols:
            cur.execute(f"ALTER TABLE {c.TABLE} ADD COLUMN name VARCHAR(255) NULL, "
                        "ALGORITHM=INSTANT")
            c.log("restore: name column re-added (nullable, so writers work NOW)")
        t0 = time.time()
        cur.execute(f"UPDATE {c.TABLE} SET name = IF(last_name = '', first_name, "
                    "CONCAT(first_name, ' ', last_name)) WHERE name IS NULL")
        c.log(f"restore: refilled {cur.rowcount} rows from first/last "
              f"in {time.time() - t0:.1f}s")
    c.log("restore done — only writes that were DUAL-written survived the outage; "
          "this exact rebuild is impossible after the naive migration")


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--force", action="store_true",
                    help="skip every precondition (the early-contract drill)")
    ap.add_argument("--relax", action="store_true",
                    help="contract step 0: make name nullable so v3 can deploy")
    ap.add_argument("--restore", action="store_true",
                    help="re-add name and refill it from first/last")
    ap.add_argument("--soak", type=float, default=8.0)
    args = ap.parse_args()

    if args.relax:
        relax()
        return 0
    if args.restore:
        restore()
        return 0
    if "name" not in c.columns():
        c.log("contract: name already dropped — nothing to do")
        return 0
    if args.force:
        c.log("contract --force: SKIPPING all preconditions (this is the drill)")
    else:
        preconditions(args.soak)
    drop()
    return 0


if __name__ == "__main__":
    sys.exit(main())
