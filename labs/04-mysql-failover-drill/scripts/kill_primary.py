"""Kill the current primary — with deterministic replication lag first.

Why inject lag at all: on a laptop, all three nodes share a kernel and
replication "lag" is microseconds, so an async crash would lose ~nothing and
the drill would be flaky theater. Real systems lose data because replicas run
seconds behind (cross-AZ hops, IO stalls, replication backlog). We reproduce
that window deterministically: STOP REPLICA IO_THREAD on both replicas — a
clean 2s network partition between primary and replicas — then SIGKILL the
primary mid-partition.

(First attempt was SIGSTOP on the replica containers. It doesn't work: the
primary's dump thread keeps pushing binlog into the frozen replica's kernel
socket buffer, and on SIGCONT the replica happily drains events from a
now-dead primary. The kernel does not care about your drill.)

With --watch-semisync, polls Rpl_semi_sync_source_status on the primary during
the partition — that's how you watch semisync silently degrade to async.
"""
import argparse
import time

import pymysql

from common import (NODES, TIMELINE, connect, current_primary, docker,
                    is_alive, log, now_ms, query_one, timeline_put)


def semisync_source_status(conn) -> str:
    row = query_one(conn, "SHOW GLOBAL STATUS LIKE 'Rpl_semi_sync_source_status'")
    return row["Value"] if row else "N/A"


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--lag-ms", type=int, default=2000,
                    help="partition replicas from the primary for this long before the kill")
    ap.add_argument("--watch-semisync", action="store_true",
                    help="poll the primary's semisync status during the partition")
    args = ap.parse_args()

    primary = current_primary()
    replicas = [n for n in NODES if n != primary and is_alive(n)]
    if not replicas:
        raise SystemExit("no live replicas — refusing to kill the only node with the data")

    TIMELINE.unlink(missing_ok=True)   # fresh timeline per drill

    if args.lag_ms:
        for node in replicas:
            with connect(node, db=None) as c, c.cursor() as cur:
                cur.execute("STOP REPLICA IO_THREAD")
        timeline_put(lag_injected_ts=now_ms())
        log.info("partition injected: %s stopped fetching from %s for %dms",
                 "+".join(replicas), primary, args.lag_ms)

        if args.watch_semisync:
            pconn = connect(primary, db=None)
            last = semisync_source_status(pconn)
            log.info("primary Rpl_semi_sync_source_status=%s", last)
            deadline = time.time() + args.lag_ms / 1000.0
            while time.time() < deadline:
                try:
                    cur_status = semisync_source_status(pconn)
                except pymysql.Error:
                    break
                if cur_status != last:
                    timeline_put(semisync_degraded_ts=now_ms())
                    log.warning("primary Rpl_semi_sync_source_status flipped %s -> %s "
                                "(semisync has SILENTLY degraded to async)", last, cur_status)
                    last = cur_status
                time.sleep(0.2)
            pconn.close()
        else:
            time.sleep(args.lag_ms / 1000.0)

    docker("kill", "-s", "KILL", NODES[primary]["container"])
    timeline_put(kill_ts=now_ms(), killed_node=primary)
    log.info("SIGKILL sent to %s — the primary is gone", NODES[primary]["container"])

    if args.lag_ms:
        # Lift the partition. The IO threads will retry against a corpse; that's
        # fine — failover.py stops them properly during promotion.
        for node in replicas:
            with connect(node, db=None) as c, c.cursor() as cur:
                cur.execute("START REPLICA IO_THREAD")
        log.info("partition lifted (replicas now retrying against a dead primary)")


if __name__ == "__main__":
    main()
