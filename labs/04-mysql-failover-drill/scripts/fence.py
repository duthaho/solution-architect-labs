"""Fence a node: make it impossible for it to accept writes, ever again,
until a human (or rebuild.py) deliberately says otherwise.

super_read_only=ON refuses writes even from SUPER/root — that's the difference
from plain read_only, and it's why the drill writer's "stale connection" starts
failing with error 1290 the moment this runs. PERSIST means a restart doesn't
un-fence it.

Also reports errant GTIDs: transactions this node executed that the current
primary never saw — the fingerprint of split-brain.
"""
import sys

from common import (connect, current_primary, gtid_executed, gtid_subtract,
                    log, query_one)


def fence_node(node: str) -> str:
    """Fence `node`; returns its errant GTID set relative to the current primary."""
    conn = connect(node, db=None)
    with conn.cursor() as cur:
        cur.execute("SET PERSIST super_read_only=ON")
        cur.execute("SET PERSIST read_only=ON")
    log.info("%s fenced: super_read_only=ON (persisted — survives restart)", node)

    primary = current_primary()
    pconn = connect(primary, db=None)
    errant = gtid_subtract(conn, gtid_executed(conn), gtid_executed(pconn))
    if errant:
        log.warning("%s holds ERRANT GTIDs (never replicated to %s): %s", node, primary, errant)
    else:
        log.info("%s has no errant GTIDs relative to %s", node, primary)
    conn.close()
    pconn.close()
    return errant


if __name__ == "__main__":
    if len(sys.argv) != 2:
        raise SystemExit("usage: fence.py <node>")
    fence_node(sys.argv[1])
