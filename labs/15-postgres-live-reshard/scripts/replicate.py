import sys
import time

from bootstrap import clear_replication
from common import (
    JOURNAL, N_SHARDS, SHARDS, conn, container_dsn, log, shard_filter,
    write_router_state,
)


def setup():
    with conn("mono") as mono:
        for i in range(N_SHARDS):
            mono.execute(
                f"CREATE PUBLICATION pub_shard{i} FOR TABLE docs WHERE {shard_filter(i)}"
            )
            log.info("mono: pub_shard%d created (filter %s)", i, shard_filter(i))
    for i, shard in enumerate(SHARDS):
        with conn(shard) as c:
            c.execute(
                f"CREATE SUBSCRIPTION sub_shard{i} "
                f"CONNECTION '{container_dsn('mono')}' PUBLICATION pub_shard{i}"
            )
            log.info("%s: sub_shard%d created (copy_data on, initial sync starts)", shard, i)


def wait_initial_sync(timeout=600):
    """Initial table sync is done when srsubstate = 'r' (ready) for all tables."""
    deadline = time.monotonic() + timeout
    for shard in SHARDS:
        with conn(shard) as c:
            while True:
                states = [s for (s,) in c.execute(
                    "SELECT srsubstate FROM pg_subscription_rel").fetchall()]
                if states and all(s == "r" for s in states):
                    break
                if time.monotonic() > deadline:
                    raise TimeoutError(f"{shard}: initial sync not ready, states={states}")
                time.sleep(0.5)
        log.info("%s: initial sync ready", shard)


def capture_lsn(node="mono"):
    with conn(node) as c:
        return c.execute("SELECT pg_current_wal_lsn()").fetchone()[0]


def wait_for_lsn(lsn, publisher="mono", subs=None, timeout=120):
    """Publisher-side gate: pg_stat_replication.replay_lsn is fed back by the
    apply worker and advances on keepalives even when no logical change
    follows the captured LSN — the subscriber-side origin LSN does not."""
    subs = subs if subs is not None else [f"sub_shard{i}" for i in range(N_SHARDS)]
    deadline = time.monotonic() + timeout
    with conn(publisher) as c:
        while True:
            rows = dict(c.execute(
                "SELECT application_name, replay_lsn >= %s::pg_lsn "
                "FROM pg_stat_replication WHERE application_name = ANY(%s)",
                (lsn, subs),
            ).fetchall())
            if len(rows) == len(subs) and all(rows.values()):
                return
            if time.monotonic() > deadline:
                raise TimeoutError(f"catch-up to {lsn} timed out: {rows}")
            time.sleep(0.1)


def status():
    with conn("mono") as c:
        current = c.execute("SELECT pg_current_wal_lsn()").fetchone()[0]
        rows = c.execute(
            "SELECT application_name, state, replay_lsn, "
            "pg_wal_lsn_diff(pg_current_wal_lsn(), replay_lsn) AS lag_bytes "
            "FROM pg_stat_replication ORDER BY application_name"
        ).fetchall()
    print(f"mono current_lsn: {current}")
    print(f"{'subscription':<14} {'state':<10} {'replay_lsn':<12} lag_bytes")
    for name, state, replay, lag in rows:
        print(f"{name:<14} {state:<10} {str(replay):<12} {lag}")
    counts = {}
    for node in ["mono"] + SHARDS:
        with conn(node) as c:
            counts[node] = c.execute("SELECT count(*) FROM docs").fetchone()[0]
    print(f"rows: {counts} (shard sum = {sum(v for k, v in counts.items() if k != 'mono')})")


def reset():
    """Back to the pre-cutover baseline: mono authoritative, shards emptied
    and re-synced from scratch, fresh journal epoch."""
    write_router_state({"authoritative": "mono", "writes_gated": False})
    clear_replication(["mono"] + SHARDS)
    for shard in SHARDS:
        with conn(shard) as c:
            c.execute("TRUNCATE docs")
            # truncate keeps sequence state: restore the pristine post-copy
            # world (sequence at its start value) or the drills lose their
            # deterministic reproduction on a second pass
            c.execute("ALTER SEQUENCE docs_id_seq RESTART WITH 1 INCREMENT BY 1")
    JOURNAL.unlink(missing_ok=True)
    log.info("shards truncated, replication cleared, journal reset — re-establishing")
    setup()
    wait_initial_sync()
    log.info("baseline restored: mono authoritative, shards in sync")


def main():
    cmd = sys.argv[1] if len(sys.argv) > 1 else "setup"
    if cmd == "setup":
        setup()
        wait_initial_sync()
        log.info("forward replication established, initial sync complete")
    elif cmd == "reset":
        reset()
    elif cmd == "status":
        status()
    elif cmd == "wait-lsn":
        lsn = capture_lsn()
        log.info("captured mono LSN %s, waiting for both shards", lsn)
        wait_for_lsn(lsn)
        log.info("both shards replayed past %s", lsn)
    else:
        raise SystemExit(f"unknown subcommand: {cmd}")


if __name__ == "__main__":
    main()
