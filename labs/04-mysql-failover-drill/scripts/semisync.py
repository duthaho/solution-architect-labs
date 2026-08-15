"""Enable/disable/inspect lossless semi-synchronous replication.

Design choices that matter (see README for the full story):

- AFTER_SYNC (the "lossless" wait point): the primary waits for a replica ack
  AFTER writing to its binlog but BEFORE committing to the storage engine and
  acking the client. A crash between binlog-write and replica-ack leaves the
  transaction NOT acked to the client and already on a replica or recoverable —
  no acked-but-lost window. AFTER_COMMIT (the pre-5.7 behavior) acks... after
  commit: other sessions can already SEE a transaction that a failover may
  lose. Phantom durability.

- Enabled on ALL nodes, both roles (source_enabled AND replica_enabled):
  roles swap during failover; the promoted replica must come up as a semisync
  SOURCE without anyone remembering to flip a switch mid-incident.

- Timeout 3000ms, deliberately low: `drill-degrade` reproduces the silent
  fallback to async in seconds instead of the default 10s.
"""
import sys

from common import NODES, connect, is_alive, log, query_one

TIMEOUT_MS = 3000


def exec_sql(conn, sql: str) -> None:
    with conn.cursor() as cur:
        cur.execute(sql)


def set_everywhere(enabled: bool) -> None:
    on = "ON" if enabled else "OFF"
    for node in NODES:
        if not is_alive(node):
            log.warning("%s is dead, skipping (semisync.py is safe to re-run after restore)", node)
            continue
        conn = connect(node, db=None)
        if enabled:
            # wait_point must be set while semisync is off; order matters.
            exec_sql(conn, "SET PERSIST rpl_semi_sync_source_enabled=OFF")
            exec_sql(conn, "SET PERSIST rpl_semi_sync_source_wait_point=AFTER_SYNC")
            exec_sql(conn, f"SET PERSIST rpl_semi_sync_source_timeout={TIMEOUT_MS}")
        exec_sql(conn, f"SET PERSIST rpl_semi_sync_source_enabled={on}")
        exec_sql(conn, f"SET PERSIST rpl_semi_sync_replica_enabled={on}")
        # a running IO thread only picks up the replica-side change on restart
        st = query_one(conn, "SHOW REPLICA STATUS")
        if st and st["Replica_IO_Running"] == "Yes":
            exec_sql(conn, "STOP REPLICA IO_THREAD")
            exec_sql(conn, "START REPLICA IO_THREAD")
        conn.close()
        log.info("%s: semisync %s (source+replica roles)", node, on)


def status() -> None:
    for node in NODES:
        if not is_alive(node):
            print(f"{node:9s} DEAD")
            continue
        conn = connect(node, db=None)
        with conn.cursor() as cur:
            cur.execute("SHOW GLOBAL STATUS LIKE 'Rpl_semi_sync%'")
            rows = {r["Variable_name"]: r["Value"] for r in cur.fetchall()}
        interesting = ["Rpl_semi_sync_source_status", "Rpl_semi_sync_source_clients",
                       "Rpl_semi_sync_replica_status", "Rpl_semi_sync_source_no_tx",
                       "Rpl_semi_sync_source_yes_tx"]
        print(f"{node:9s} " + "  ".join(f"{k.removeprefix('Rpl_semi_sync_')}={rows[k]}"
                                        for k in interesting if k in rows))
        conn.close()


if __name__ == "__main__":
    cmd = sys.argv[1] if len(sys.argv) > 1 else "status"
    if cmd == "enable":
        set_everywhere(True)
    elif cmd == "disable":
        set_everywhere(False)
    elif cmd == "status":
        status()
    else:
        raise SystemExit(f"usage: semisync.py enable|disable|status (got {cmd!r})")
