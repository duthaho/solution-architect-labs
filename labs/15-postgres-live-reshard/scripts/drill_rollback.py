"""The rollback that still works AFTER the cutover: the reverse stream armed
inside the cutover gate has been feeding every post-cutover shard write back
to the monolith. Take live writes on the shards, then roll back — gate,
wait for the monolith to replay past both shards' LSNs, flip routing home —
and prove every write the shards ever acked survived the return."""
import threading
import time

from common import (
    JOURNAL, SHARDS, conn, drain_writes, log, read_jsonl, read_router_state,
    write_router_state,
)
from replicate import wait_for_lsn
from traffic import Traffic

TAKE_SECONDS = 4.0


def audit_on_mono(records):
    damage = []
    with conn("mono") as c:
        for r in records:
            row = c.execute(
                "SELECT rev FROM docs WHERE workspace_id = %s AND id = %s",
                (r["ws"], r["id"]),
            ).fetchone()
            if row is None:
                damage.append({**r, "damage": "missing"})
            elif row[0] < r["rev"]:
                damage.append({**r, "damage": "stale", "mono_rev": row[0]})
    return damage


def rollback():
    t0 = time.monotonic()
    log.info("gate: pausing writes at the router")
    state = read_router_state()
    state["writes_gated"] = True
    write_router_state(state)
    try:
        drain_writes()

        for i, shard in enumerate(SHARDS):
            with conn(shard) as c:
                lsn = c.execute("SELECT pg_current_wal_lsn()").fetchone()[0]
            log.info("%s quiesced at %s — waiting for mono to replay past it", shard, lsn)
            wait_for_lsn(lsn, publisher=shard, subs=[f"sub_back{i}"])

        # The sequence trap, mirrored: the shards minted ids mono's sequence
        # has never seen — reverse-applied rows sit ABOVE it, and mono's next
        # inserts would walk straight into them. Re-syncing the sequence is as
        # much a part of rollback as the routing flip.
        with conn("mono") as c:
            new_max = c.execute(
                "SELECT setval('docs_id_seq', (SELECT max(id) FROM docs))"
            ).fetchone()[0]
        log.info("mono docs_id_seq re-synced to %d (above every shard-minted id)", new_max)
    except BaseException:
        # gate must never stay wedged: shards remain authoritative, writes
        # resume, and the reverse stream keeps feeding mono for the next try
        state["writes_gated"] = False
        write_router_state(state)
        log.error("rollback FAILED — gate released, shards still authoritative")
        raise

    log.info("mono has replayed everything both shards ever acked — flipping home")
    write_router_state({"authoritative": "mono", "writes_gated": False})
    return time.monotonic() - t0


def main():
    state = read_router_state()
    if state["authoritative"] != "shards":
        raise SystemExit("run after the gated cutover: shards must be authoritative")
    for i in range(len(SHARDS)):
        with conn("mono") as c:
            if not c.execute(
                "SELECT 1 FROM pg_subscription WHERE subname = %s", (f"sub_back{i}",)
            ).fetchone():
                raise SystemExit(f"reverse stream sub_back{i} missing — cutover did not arm it")

    offset = len(read_jsonl(JOURNAL))
    stop = threading.Event()
    traffic = Traffic()
    thread = threading.Thread(target=traffic.run, args=(stop,), daemon=True)
    thread.start()
    log.info("taking live writes on the shards for %.0fs", TAKE_SECONDS)
    time.sleep(TAKE_SECONDS)

    pause = rollback()
    time.sleep(1.0)  # traffic continues, back against mono
    stop.set()
    thread.join()

    shard_writes = [r for r in read_jsonl(JOURNAL)[offset:] if r["node"] in SHARDS]
    damage = audit_on_mono(shard_writes)

    print()
    print(f"write pause during rollback        : {pause * 1000:.0f} ms")
    print(f"writes acked by shards, audited on mono: {len(shard_writes)}")
    inserts = sum(1 for r in shard_writes if r["op"] == "insert")
    print(f"  of which brand-new docs (inserts): {inserts}")
    print(f"missing or stale on mono           : {len(damage)}")
    print()
    if damage:
        print(f"❌ ROLLBACK LOST DATA: {len(damage)} shard-acked writes not on mono")
        raise SystemExit(1)
    if not shard_writes:
        print("❌ no shard writes were taken — drill proved nothing")
        raise SystemExit(1)
    print("✅ rolled back after the cutover without losing one shard-acked write —")
    print("   the reverse stream armed inside the cutover gate made the new side")
    print("   as recoverable as the old one. (Figma/Notion kept exactly this.)")


if __name__ == "__main__":
    main()
