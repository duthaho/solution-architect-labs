"""Rejoin a dead or divergent node as a replica of the current primary.

The honest move after a failover (and the ONLY honest move after split-brain):
throw the node's local history away and rebuild from the survivor lineage.
Any acked-but-lost rows and any zombie writes it holds are NOT merged — they
are evidence for the incident report (verify.py already printed them), not
data to be quietly resurrected next to a primary that never saw them.

Why CLONE and not binlog replay: a tempting "cheap" rebuild is RESET MASTER +
auto-position from an empty GTID set, letting replication replay all history.
It cannot work for an ex-primary: this node AUTHORED part of that history, and
a replica silently discards binlog events carrying its own server_id (the
circular-replication guard). It skips its own old transactions, then dies on
row events against a table whose CREATE it just skipped. A rebuild must come
from a SNAPSHOT — which is exactly what the CLONE plugin is: physical
InnoDB-page transfer from a donor, gtid_executed included, then the delta via
normal replication. Same idea at 500M rows, just a longer copy.

Mechanics note: at the end of a clone the recipient restarts itself; in a
container (mysqld is PID 1, no supervisor) that means the container EXITS and
we docker-start it again. Expected, handled below.
"""
import sys
import time

import pymysql

from common import (NODES, REPL_PASSWORD, REPL_USER, connect, current_primary,
                    docker, gtid_executed, gtid_subtract, is_alive, log,
                    query_one, replica_status)


def exec_sql(conn, sql: str, args=None) -> None:
    with conn.cursor() as cur:
        cur.execute(sql, args)


def wait_alive(node: str, timeout_s: int = 120) -> None:
    deadline = time.time() + timeout_s
    while not is_alive(node):
        if time.time() > deadline:
            raise SystemExit(f"{node} did not come up within {timeout_s}s")
        time.sleep(2)


def rebuild_node(node: str) -> None:
    primary = current_primary()
    assert node != primary, f"refusing to rebuild the current primary ({node})"

    if not is_alive(node):
        log.info("%s is down — starting its container", node)
        docker("start", NODES[node]["container"])
        wait_alive(node)

    donor_set = gtid_executed(connect(primary, db=None))

    conn = connect(node, db=None)
    exec_sql(conn, "STOP REPLICA")
    exec_sql(conn, "RESET REPLICA ALL")
    # clone must be able to drop + replace local data: unfence for the copy
    # (the node is unreachable to clients for the whole clone anyway)
    exec_sql(conn, "SET GLOBAL super_read_only=OFF")
    exec_sql(conn, "SET GLOBAL read_only=OFF")

    donor = f"{NODES[primary]['service']}:3306"
    exec_sql(conn, "SET GLOBAL clone_valid_donor_list=%s", (donor,))
    log.info("%s: cloning full snapshot from %s (wipes local data, divergent history included)...",
             node, donor)
    try:
        exec_sql(conn, f"CLONE INSTANCE FROM '{REPL_USER}'@'{NODES[primary]['service']}':3306 "
                       f"IDENTIFIED BY '{REPL_PASSWORD}'")
    except pymysql.err.Error as e:
        # The server shuts down at the end of a successful clone; losing the
        # connection here IS the success path (2013/2006 = gone mid-query,
        # 3707 = "restart failed: not managed by supervisor", i.e. it shut
        # down and the container exited). Anything else is a real failure.
        if e.args[0] not in (2013, 2006, 3707):
            raise
        log.info("connection dropped as the recipient shut down post-clone (expected): %s", e.args[:1])
    conn.close()

    # mysqld was PID 1, so "restart" = container exit; bring it back.
    time.sleep(3)
    for _ in range(3):
        if is_alive(node):
            break
        docker("start", NODES[node]["container"])
        time.sleep(5)
    wait_alive(node)

    conn = connect(node, db=None)
    got = gtid_executed(conn)
    log.info("%s back up post-clone, gtid_executed=%s", node, got or "(empty)")
    # tripwire: if the clone silently didn't happen, the node still has its old
    # (possibly divergent) history instead of the donor's — refuse to proceed
    missing = gtid_subtract(conn, donor_set, got)
    if missing:
        raise SystemExit(f"{node} post-clone is missing donor transactions ({missing}) — clone failed")

    # fence FIRST — a rebuilding node must never take writes, and clone does
    # not carry over the donor's read-only settings
    exec_sql(conn, "SET PERSIST read_only=ON")
    exec_sql(conn, "SET PERSIST super_read_only=ON")

    exec_sql(conn, """
        CHANGE REPLICATION SOURCE TO
          SOURCE_HOST=%s, SOURCE_PORT=3306,
          SOURCE_USER=%s, SOURCE_PASSWORD=%s,
          SOURCE_AUTO_POSITION=1,
          SOURCE_CONNECT_RETRY=2, SOURCE_RETRY_COUNT=86400
    """, (NODES[primary]["service"], REPL_USER, REPL_PASSWORD))
    exec_sql(conn, "START REPLICA")

    target = gtid_executed(connect(primary, db=None))
    res = query_one(conn, "SELECT WAIT_FOR_EXECUTED_GTID_SET(%s, 120) AS r", (target,))
    if res["r"] != 0:
        raise SystemExit(f"{node} did not catch up to the primary within 120s")
    st = replica_status(conn)
    assert st["Replica_IO_Running"] == "Yes" and st["Replica_SQL_Running"] == "Yes"
    log.info("%s rebuilt: cloned from %s + delta replicated, fenced as replica (executed=%s)",
             node, primary, gtid_executed(conn))
    conn.close()


def main() -> None:
    if len(sys.argv) > 1:
        targets = [sys.argv[1]]
    else:
        # restore mode: any node that is dead, or alive but not a healthy
        # replica of the current primary (e.g. a fenced zombie), gets rebuilt
        primary = current_primary()
        targets = []
        for node in NODES:
            if node == primary:
                continue
            if not is_alive(node):
                targets.append(node)
                continue
            st = replica_status(connect(node, db=None))
            healthy = (st and st["Replica_IO_Running"] == "Yes"
                       and st["Replica_SQL_Running"] == "Yes"
                       and st["Source_Host"] == NODES[primary]["service"])
            if not healthy:
                targets.append(node)
    if not targets:
        log.info("nothing to restore: all nodes are healthy replicas of %s", current_primary())
        return
    for node in targets:
        rebuild_node(node)


if __name__ == "__main__":
    main()
