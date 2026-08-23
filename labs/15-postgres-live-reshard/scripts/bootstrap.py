from common import LAB_DIR, NODES, JOURNAL, ROUTER_STATE, conn, log


def drop_replication(c):
    for (sub,) in c.execute("SELECT subname FROM pg_subscription").fetchall():
        c.execute(f"ALTER SUBSCRIPTION {sub} DISABLE")
        c.execute(f"ALTER SUBSCRIPTION {sub} SET (slot_name = NONE)")
        c.execute(f"DROP SUBSCRIPTION {sub}")
    for (pub,) in c.execute("SELECT pubname FROM pg_publication").fetchall():
        c.execute(f"DROP PUBLICATION {pub}")
    for (slot,) in c.execute(
        "SELECT slot_name FROM pg_replication_slots WHERE active = false"
    ).fetchall():
        c.execute("SELECT pg_drop_replication_slot(%s)", (slot,))


def main():
    schema = (LAB_DIR / "sql" / "schema.sql").read_text()
    for node in NODES:
        with conn(node) as c:
            drop_replication(c)
            c.execute(schema)
            log.info("%s: schema applied (docs dropped + recreated, replication cleared)", node)
    for artifact in (JOURNAL, ROUTER_STATE):
        artifact.unlink(missing_ok=True)
    log.info("journal + router state reset")


if __name__ == "__main__":
    main()
