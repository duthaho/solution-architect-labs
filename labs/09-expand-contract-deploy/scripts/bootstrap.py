"""Create the v1 schema, initialize deploy state, and prove the split rule.

Idempotent: safe to re-run at any point. It never DROPs anything — reset.py
owns destructive restoration.
"""
import common as c


def assert_split_agreement() -> None:
    """The name-split rule exists in three places: app.py (Python), common.py
    (Python), and backfill.py (SQL). If SQL and Python ever disagree, the
    backfilled rows and the dual-written rows diverge silently — so refuse to
    run until they provably agree on the nasty cases."""
    cases = ["Ada Lovelace", "Mary Jane Watson", "Prince", "A B C D", " leading",
             "trailing ", "double  space"]
    with c.connect().cursor() as cur:
        for name in cases:
            cur.execute(f"SELECT {c.SQL_FIRST}, {c.SQL_LAST} FROM "
                        "(SELECT %s AS name) t", (name,))
            sql_pair = cur.fetchone()
            py_pair = c.split_name(name)
            assert tuple(sql_pair) == py_pair, \
                f"split disagreement on {name!r}: sql={sql_pair} py={py_pair}"
    c.log(f"split rule: SQL and Python agree on {len(cases)} cases")


def main() -> None:
    c.wait_for_mysql()
    ddl = (c.LAB_DIR / "sql" / "v1.sql").read_text()
    with c.connect().cursor() as cur:
        for stmt in [s.strip() for s in ddl.split(";") if s.strip()]:
            cur.execute(stmt)
    c.log(f"schema applied: {c.TABLE}({', '.join(sorted(c.columns()))})")

    assert_split_agreement()

    # Deploy state: blue pair, v1, in the load balancer. (The Makefile already
    # wrote a default upstreams.conf so the gateway could boot; rewrite it
    # anyway so bootstrap is authoritative.)
    for pod in c.PODS:
        c.write_pod_version(pod, "v1")
    c.write_deploy_state({"active_color": "blue", "versions": {p: "v1" for p in c.PODS}})
    c.write_upstreams(c.COLORS["blue"])
    c.log("deploy state: blue pair active, all pods pinned v1")


if __name__ == "__main__":
    main()
