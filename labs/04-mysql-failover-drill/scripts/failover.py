"""The promotion algorithm. This is the heart of the lab — what orchestrator/MHA
do, in readable form:

  1. confirm the primary is actually dead (short-timeout probe)
  2. poll survivors: what has each RECEIVED (retrieved ∪ executed)?
  3. candidate = the replica that received the most
  4. drain: wait until the candidate has APPLIED everything it received
  5. sanity: no other survivor may hold GTIDs the candidate lacks
  6. promote: stop replication, drop read-only (persisted)
  7. repoint the other replicas at the new primary (GTID auto-position)
  8. atomically flip the router
  9. print the timeline: that's your RTO

Step 3 compares RECEIVED, not applied: a replica that has everything in its
relay log but hasn't applied it yet loses nothing — it just needs step 4.
Step 5 is the errant-GTID tripwire: promoting a candidate that is MISSING
transactions another survivor has means those transactions can never be
replicated again cleanly. Run `--force-candidate replica2` after lagging it to
watch that mess happen on purpose (drill 5 in the README).
"""
import argparse
import sys
import time

from common import (NODES, connect, current_primary, gtid_executed, gtid_subtract,
                    gtid_union, is_alive, log, now_ms, query_one, replica_status,
                    retrieved_gtid_set, timeline_put, write_router)


def exec_sql(conn, sql: str, args=None) -> None:
    with conn.cursor() as cur:
        cur.execute(sql, args)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--force-candidate", default=None, choices=list(NODES),
                    help="promote this node regardless of GTID comparison (to practice doing it wrong)")
    args = ap.parse_args()

    # 1. Confirm the primary is dead. Never promote while it might be alive:
    #    two writable primaries is the disease, failover is supposed to be the cure.
    dead = current_primary()
    if is_alive(dead):
        raise SystemExit(f"{dead} still answers on port {NODES[dead]['port']} — "
                         "refusing to promote against a live primary")
    timeline_put(detect_ts=now_ms())
    log.info("confirmed: %s is dead", dead)

    survivors = [n for n in NODES if n != dead and is_alive(n)]
    if not survivors:
        raise SystemExit("no survivors — this is a restore-from-backup day, not a failover")
    conns = {n: connect(n, db=None) for n in survivors}

    # 2. What has each survivor RECEIVED? (retrieved ∪ executed)
    effective: dict[str, str] = {}
    for n in survivors:
        exec_sql(conns[n], "STOP REPLICA IO_THREAD")   # freeze the comparison
        eff = gtid_union(conns[n], gtid_executed(conns[n]), retrieved_gtid_set(conns[n]))
        effective[n] = eff
        log.info("%s has (received ∪ executed): %s", n, eff or "(empty)")

    # 3. Candidate = maximal effective set (set-containment, not string length).
    if args.force_candidate:
        candidate = args.force_candidate
        log.warning("candidate FORCED to %s — skipping GTID comparison", candidate)
    else:
        candidate = survivors[0]
        ref = conns[candidate]
        for n in survivors[1:]:
            if gtid_subtract(ref, effective[n], effective[candidate]):
                # n has GTIDs the current candidate lacks
                if gtid_subtract(ref, effective[candidate], effective[n]):
                    raise SystemExit(
                        f"DIVERGED SURVIVORS: {candidate} and {n} each hold GTIDs the "
                        "other lacks. No safe automatic promotion exists — a human "
                        "must pick which history wins and rebuild the other node.")
                candidate = n
    timeline_put(candidate_ts=now_ms(), candidate=candidate)
    log.info("promotion candidate: %s", candidate)

    # 4. Drain the relay log: everything received must be applied before writes.
    cconn = conns[candidate]
    retrieved = retrieved_gtid_set(cconn)
    if retrieved:
        t0 = time.time()
        res = query_one(cconn, "SELECT WAIT_FOR_EXECUTED_GTID_SET(%s, 60) AS r", (retrieved,))
        if res["r"] != 0:
            raise SystemExit(f"{candidate} failed to apply its relay log within 60s")
        log.info("%s drained its relay log in %.3fs", candidate, time.time() - t0)
    timeline_put(relay_drained_ts=now_ms())

    # 5. Tripwire: no survivor may hold transactions the candidate lacks.
    cand_set = gtid_executed(cconn)
    for n in survivors:
        if n == candidate:
            continue
        missing = gtid_subtract(cconn, effective[n], cand_set)
        if missing:
            log.error("!! %s holds GTIDs the new primary will NOT have: %s", n, missing)
            log.error("!! these transactions are about to become errant — promoting "
                      "anyway because --force-candidate said so" if args.force_candidate
                      else "!! aborting")
            if not args.force_candidate:
                sys.exit(1)

    # 6. Promote. PERSIST: if the new primary restarts it must stay writable.
    exec_sql(cconn, "STOP REPLICA")
    exec_sql(cconn, "RESET REPLICA ALL")
    exec_sql(cconn, "SET PERSIST super_read_only=OFF")
    exec_sql(cconn, "SET PERSIST read_only=OFF")
    timeline_put(promoted_ts=now_ms())
    log.info("%s promoted: replication config cleared, node writable", candidate)

    # 7. Repoint the other survivors at the new primary.
    for n in survivors:
        if n == candidate:
            continue
        c = conns[n]
        exec_sql(c, "STOP REPLICA")
        exec_sql(c, """
            CHANGE REPLICATION SOURCE TO
              SOURCE_HOST=%s, SOURCE_PORT=3306,
              SOURCE_USER='repl', SOURCE_PASSWORD='repl',
              SOURCE_AUTO_POSITION=1,
              SOURCE_CONNECT_RETRY=2, SOURCE_RETRY_COUNT=86400
        """, (NODES[candidate]["service"],))
        exec_sql(c, "START REPLICA")
        st = replica_status(c)
        log.info("%s repointed -> %s (IO=%s SQL=%s)", n, candidate,
                 st["Replica_IO_Running"], st["Replica_SQL_Running"])

    # 8. Flip the router. From this instant the writer's next reconnect succeeds.
    write_router(candidate)
    timeline_put(router_flip_ts=now_ms())

    for c in conns.values():
        c.close()
    log.info("failover complete: %s is the new primary", candidate)


if __name__ == "__main__":
    main()
