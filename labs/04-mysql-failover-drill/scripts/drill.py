"""Drill orchestrator: runs one complete failure scenario end-to-end.

    drill.py async      async replication, 2s partition, kill  -> expect loss > 0
    drill.py semisync   same crash, semisync on                -> expect loss = 0
    drill.py degrade    6s partition > 3s semisync timeout     -> loss AGAIN
    drill.py zombie     resurrect old primary unfenced -> split-brain -> fence -> rebuild

Each drill assumes a healthy 1-primary + 2-replica topology (any node may hold
any role — the router knows). After a drill the killed node is left dead;
`make restore` resurrects and rejoins it.
"""
import json
import subprocess
import sys
import time
from pathlib import Path

from common import (JOURNAL, LAB_DIR, NODES, connect, current_primary, docker,
                    gtid_executed, gtid_subtract, is_alive, log, query_one)

PY = sys.executable
SCRIPTS = Path(__file__).parent


def sh(*argv: str) -> None:
    subprocess.run(argv, check=True)


def preflight() -> None:
    primary = current_primary()
    dead = [n for n in NODES if not is_alive(n)]
    if dead:
        raise SystemExit(f"{dead} not running — run `make restore` (or `make up bootstrap`) first")
    conn = connect(primary)
    # scrub previous drill's traffic rows so the journal is the whole truth
    with conn.cursor() as cur:
        cur.execute("DELETE FROM events WHERE payload LIKE 'traffic-%%' OR payload LIKE 'zombie-%%'")
    conn.close()
    JOURNAL.unlink(missing_ok=True)
    log.info("preflight ok: %s is primary, all nodes alive, journal cleared", primary)


def traffic_start(*extra: str) -> subprocess.Popen:
    logf = open(LAB_DIR / "traffic.log", "a")
    return subprocess.Popen([PY, str(SCRIPTS / "traffic.py"), *extra],
                            stdout=logf, stderr=subprocess.STDOUT)


def traffic_stop(proc: subprocess.Popen) -> None:
    proc.terminate()
    proc.wait(timeout=15)


def crash_drill(label: str, lag_ms: int, semisync: bool, watch: bool = False) -> None:
    preflight()
    sh(PY, str(SCRIPTS / "semisync.py"), "enable" if semisync else "disable")
    writer = traffic_start()
    log.info("writer running for 4s before the incident...")
    time.sleep(4)
    args = [PY, str(SCRIPTS / "kill_primary.py"), "--lag-ms", str(lag_ms)]
    if watch:
        args.append("--watch-semisync")
    sh(*args)
    sh(PY, str(SCRIPTS / "failover.py"))
    log.info("letting the writer reconnect and write to the new primary for 4s...")
    time.sleep(4)
    traffic_stop(writer)
    sh(PY, str(SCRIPTS / "verify.py"), "--label", label)


def zombie_drill() -> None:
    """Split-brain on purpose. Requires a completed failover (one dead ex-primary)."""
    import fence
    import rebuild
    dead = [n for n in NODES if not is_alive(n)]
    if len(dead) != 1:
        raise SystemExit(f"zombie drill needs exactly one dead ex-primary (found dead: {dead or 'none'}). "
                         "Run `make drill-async` first.")
    zombie = dead[0]
    primary = current_primary()

    log.info("=== resurrecting %s UNFENCED (it still thinks it is a primary) ===", zombie)
    docker("start", NODES[zombie]["container"])
    deadline = time.time() + 120
    while not is_alive(zombie):
        if time.time() > deadline:
            raise SystemExit(f"{zombie} did not come back")
        time.sleep(2)
    ro = query_one(connect(zombie, db=None), "SELECT @@read_only AS ro")["ro"]
    log.info("%s is back. read_only=%d  <-- an UNFENCED zombie, happy to take writes", zombie, ro)

    log.info("=== a stale writer (cached connection config) keeps writing to it ===")
    stale = traffic_start("--node", zombie, "--tag", "zombie", "--no-journal",
                          "--max-rows", "50", "--sleep-ms", "10")
    stale.wait(timeout=60)

    zconn = connect(zombie, db=None)
    pconn = connect(primary, db=None)
    errant = gtid_subtract(zconn, gtid_executed(zconn), gtid_executed(pconn))
    n_rows = query_one(connect(zombie), "SELECT COUNT(*) AS c FROM events WHERE payload LIKE 'zombie-%%'")["c"]
    print()
    print("=" * 64)
    print("  SPLIT-BRAIN DETECTED")
    print(f"  errant GTIDs on {zombie} (GTID_SUBTRACT(zombie, {primary})):")
    print(f"      {errant or '(none)'}")
    print(f"  divergent rows on {zombie}: {n_rows}")
    print("  two nodes both believe they are the source of truth.")
    print("=" * 64)
    if not errant:
        raise SystemExit("expected errant GTIDs on the zombie — drill failed")

    log.info("=== fencing the zombie ===")
    fence.fence_node(zombie)

    log.info("=== rebuilding: wipe local history, rejoin as replica of %s ===", primary)
    rebuild.rebuild_node(zombie)

    # prove convergence: zombie's divergent rows are gone, GTID sets match
    zconn = connect(zombie, db=None)
    errant_after = gtid_subtract(zconn, gtid_executed(zconn), gtid_executed(pconn))
    zc = query_one(connect(zombie), "SELECT COUNT(*) AS c FROM events")["c"]
    pc = query_one(connect(primary), "SELECT COUNT(*) AS c FROM events")["c"]
    print()
    print("=" * 64)
    print("  AFTER FENCE + REBUILD")
    print(f"  errant GTIDs on {zombie}: {errant_after or '(none)'}")
    print(f"  row counts: {zombie}={zc}  {primary}={pc}  match={zc == pc}")
    print("=" * 64)
    if errant_after or zc != pc:
        raise SystemExit("zombie did not converge after rebuild")
    log.info("zombie drill complete: divergence detected, fenced, rebuilt, converged")


def main() -> None:
    mode = sys.argv[1] if len(sys.argv) > 1 else "async"
    if mode == "async":
        crash_drill("async", lag_ms=2000, semisync=False)
    elif mode == "semisync":
        crash_drill("semisync", lag_ms=2000, semisync=True)
    elif mode == "degrade":
        crash_drill("degrade", lag_ms=6000, semisync=True, watch=True)
    elif mode == "zombie":
        zombie_drill()
    else:
        raise SystemExit(f"unknown drill {mode!r}")


if __name__ == "__main__":
    main()
