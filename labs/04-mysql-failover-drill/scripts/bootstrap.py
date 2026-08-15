"""Wire the 3-node topology from scratch (and reset it if it was already wired).

Why the RESET MASTER everywhere: the mysql docker entrypoint's own init
(create db, root user setup) runs with binlog enabled, so every node boots with
a few GTIDs under its OWN server_uuid. Left in place, those pollute every GTID
comparison this lab is about. Since all three nodes ran the identical init,
their data is identical — so we erase all three GTID histories and start the
replicated world from an empty set. After this, every transaction in existence
originates on the primary (until the drills make things interesting).
"""
import time

from common import (DB, LAB_DIR, NODES, REPL_PASSWORD, REPL_USER, connect,
                    gtid_executed, log, query_one, replica_status, wait_for_node,
                    write_router)

REPLICAS = ["replica1", "replica2"]


def exec_sql(conn, sql: str, args=None) -> None:
    with conn.cursor() as cur:
        cur.execute(sql, args)


def create_repl_user(conn) -> None:
    """Every node gets the repl user locally (sql_log_bin=0: never replicated,
    because after RESET MASTER each node must still have it independently —
    any node can become the source later."""
    exec_sql(conn, "SET sql_log_bin=0")
    exec_sql(conn, f"DROP USER IF EXISTS '{REPL_USER}'@'%'")
    exec_sql(conn, f"CREATE USER '{REPL_USER}'@'%' IDENTIFIED WITH mysql_native_password BY '{REPL_PASSWORD}'")
    exec_sql(conn, f"GRANT REPLICATION SLAVE ON *.* TO '{REPL_USER}'@'%'")
    # BACKUP_ADMIN: lets this user serve as a CLONE donor during rebuilds
    exec_sql(conn, f"GRANT BACKUP_ADMIN ON *.* TO '{REPL_USER}'@'%'")
    exec_sql(conn, "SET sql_log_bin=1")


def main() -> None:
    for node in NODES:
        wait_for_node(node)

    conns = {node: connect(node, db=None) for node in NODES}

    # -- full topology reset (makes bootstrap idempotent from any prior state) --
    for node, conn in conns.items():
        exec_sql(conn, "STOP REPLICA")
        exec_sql(conn, "RESET REPLICA ALL")
        exec_sql(conn, "SET PERSIST super_read_only=OFF")
        exec_sql(conn, "SET PERSIST read_only=OFF")
        create_repl_user(conn)
        exec_sql(conn, "RESET MASTER")          # wipe binlog + gtid_executed
        log.info("%s: history reset, gtid_executed=%r", node, gtid_executed(conn))

    # -- wire replicas -> primary with GTID auto-positioning --
    for node in REPLICAS:
        conn = conns[node]
        exec_sql(conn, """
            CHANGE REPLICATION SOURCE TO
              SOURCE_HOST=%s, SOURCE_PORT=3306,
              SOURCE_USER=%s, SOURCE_PASSWORD=%s,
              SOURCE_AUTO_POSITION=1,
              SOURCE_CONNECT_RETRY=2, SOURCE_RETRY_COUNT=86400
        """, (NODES["primary"]["service"], REPL_USER, REPL_PASSWORD))
        exec_sql(conn, "START REPLICA")
        # PERSIST, not GLOBAL: a replica that restarts must come back fenced.
        exec_sql(conn, "SET PERSIST read_only=ON")
        exec_sql(conn, "SET PERSIST super_read_only=ON")
        log.info("%s: replicating from primary, super_read_only=ON (persisted)", node)

    # -- schema on the primary (replicates down) --
    schema = "\n".join(l for l in (LAB_DIR / "sql" / "schema.sql").read_text().splitlines()
                       if not l.lstrip().startswith("--"))
    primary = conns["primary"]
    exec_sql(primary, f"USE {DB}")
    for stmt in filter(None, (s.strip() for s in schema.split(";"))):
        exec_sql(primary, stmt)
    log.info("schema applied on primary")

    # -- prove the pipe works: replicas must apply everything the primary has --
    target = gtid_executed(primary)
    for node in REPLICAS:
        res = query_one(conns[node], "SELECT WAIT_FOR_EXECUTED_GTID_SET(%s, 30) AS r", (target,))
        if res["r"] != 0:
            raise RuntimeError(f"{node} did not catch up to {target!r} within 30s")
        st = replica_status(conns[node])
        assert st["Replica_IO_Running"] == "Yes" and st["Replica_SQL_Running"] == "Yes", \
            f"{node} replication threads not running: {st}"
        log.info("%s: caught up (executed=%s)", node, gtid_executed(conns[node]))

    write_router("primary")
    for conn in conns.values():
        conn.close()
    log.info("bootstrap complete: primary(:3307) -> replica1(:3308), replica2(:3309)")
    # tiny settle so START REPLICA state is quiescent before drills stomp on it
    time.sleep(1)


if __name__ == "__main__":
    main()
