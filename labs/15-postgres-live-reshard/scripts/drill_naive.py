"""The naive cutover: flip the router to the shards while replication is
lagging, without a quiesce or an LSN gate, then decommission the old primary.
Every write the monolith acked during the lag window is now missing or stale
on the shards — deterministically counted from the journal."""
import threading
import time

from common import (
    JOURNAL, LAB_DIR, SHARDS, append_jsonl, conn, log, read_jsonl,
    shard_for, write_router_state,
)
from traffic import Traffic

NAIVE = LAB_DIR / "naive.jsonl"
STALL_SECONDS = 3.0


def set_subscriptions(enabled):
    for i, shard in enumerate(SHARDS):
        with conn(shard) as c:
            c.execute(f"ALTER SUBSCRIPTION sub_shard{i} {'ENABLE' if enabled else 'DISABLE'}")


def drop_subscriptions():
    for i, shard in enumerate(SHARDS):
        with conn(shard) as c:
            c.execute(f"DROP SUBSCRIPTION sub_shard{i}")


def audit(records):
    """Check every journaled acked write against the shard that now owns it."""
    damage = []
    conns = {s: conn(s) for s in SHARDS}
    try:
        for r in records:
            shard = shard_for(r["ws"])
            row = conns[shard].execute(
                "SELECT rev FROM docs WHERE workspace_id = %s AND id = %s",
                (r["ws"], r["id"]),
            ).fetchone()
            if row is None:
                damage.append({**r, "damage": "missing", "shard": shard})
            elif row[0] < r["rev"]:
                damage.append({**r, "damage": "stale", "shard": shard, "shard_rev": row[0]})
    finally:
        for c in conns.values():
            c.close()
    return damage


def main():
    NAIVE.unlink(missing_ok=True)
    write_router_state({"authoritative": "mono", "writes_gated": False})

    stop = threading.Event()
    traffic = Traffic()
    thread = threading.Thread(target=traffic.run, args=(stop,), daemon=True)
    thread.start()
    time.sleep(1.0)

    log.info("injecting replication stall (subscriptions disabled) — the lag "
             "any real system has, made deterministic")
    set_subscriptions(False)
    offset = len(read_jsonl(JOURNAL))
    time.sleep(STALL_SECONDS)

    log.info("NAIVE FLIP: routing to shards with no quiesce and no LSN gate")
    write_router_state({"authoritative": "shards", "writes_gated": False})
    lag_window = read_jsonl(JOURNAL)[offset:]
    acked_in_window = [r for r in lag_window if r["node"] == "mono"]

    log.info("naive decommission: dropping the subscriptions (operator "
             "believes the migration is done)")
    drop_subscriptions()

    stop.set()
    thread.join()

    damage = audit(acked_in_window)
    for d in damage:
        append_jsonl(NAIVE, d)
    append_jsonl(NAIVE, {
        "summary": True,
        "acked_in_window": len(acked_in_window),
        "missing": sum(1 for d in damage if d["damage"] == "missing"),
        "stale": sum(1 for d in damage if d["damage"] == "stale"),
    })

    print()
    print(f"acked by mono during the lag window : {len(acked_in_window)}")
    print(f"missing on the owning shard         : {sum(1 for d in damage if d['damage'] == 'missing')}")
    print(f"stale on the owning shard           : {sum(1 for d in damage if d['damage'] == 'stale')}")
    print()
    if not damage:
        print("❌ drill failed to reproduce damage — no acked write was lost")
        raise SystemExit(1)
    print(f"❌ {len(damage)} acked writes are GONE or STALE on the new authority —")
    print("   the monolith acked them, the shards never saw them, and the")
    print("   subscriptions that could have delivered them are dropped.")
    print("   (evidence journaled to naive.jsonl — run `make reset-shards` next)")


if __name__ == "__main__":
    main()
