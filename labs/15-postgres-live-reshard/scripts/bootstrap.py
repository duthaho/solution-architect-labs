from common import LAB_DIR, NODES, JOURNAL, ROUTER_STATE, conn, log


def drop_subscriptions(c):
    """Local-only teardown: works even when the publisher is unreachable,
    at the cost of orphaning the remote slot — which is why callers must
    sweep slots on every node AFTER all subscriptions are gone."""
    for (sub,) in c.execute("SELECT subname FROM pg_subscription").fetchall():
        c.execute(f"ALTER SUBSCRIPTION {sub} DISABLE")
        c.execute(f"ALTER SUBSCRIPTION {sub} SET (slot_name = NONE)")
        c.execute(f"DROP SUBSCRIPTION {sub}")


def drop_pubs_and_slots(c):
    for (pub,) in c.execute("SELECT pubname FROM pg_publication").fetchall():
        c.execute(f"DROP PUBLICATION {pub}")
    for (slot,) in c.execute(
        "SELECT slot_name FROM pg_replication_slots WHERE active = false"
    ).fetchall():
        c.execute("SELECT pg_drop_replication_slot(%s)", (slot,))


def clear_replication(nodes):
    """Two passes over the whole topology: every subscription everywhere
    first (their walsenders release the remote slots), then publications and
    the now-inactive slots. One combined pass in node order orphans the
    forward slots on mono — the exact trap the README warns about."""
    for node in nodes:
        with conn(node) as c:
            drop_subscriptions(c)
    for node in nodes:
        with conn(node) as c:
            drop_pubs_and_slots(c)


def main():
    schema = (LAB_DIR / "sql" / "schema.sql").read_text()
    clear_replication(list(NODES))
    for node in NODES:
        with conn(node) as c:
            c.execute(schema)
            log.info("%s: schema applied (docs dropped + recreated, replication cleared)", node)
    for artifact in (JOURNAL, ROUTER_STATE):
        artifact.unlink(missing_ok=True)
    log.info("journal + router state reset")


if __name__ == "__main__":
    main()
