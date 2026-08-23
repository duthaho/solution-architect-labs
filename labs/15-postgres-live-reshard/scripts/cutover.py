"""The gated cutover, Figma/Notion style: gate writes at the router, capture
the monolith's LSN once quiesced, wait until every shard has replayed past it,
flip the replication streams (forward subs dropped, reverse stream started —
INSIDE the gate, so not one post-cutover write can escape it), flip the
routing state atomically, resume. The pause is seconds; the loss is zero —
and the same audit that condemned the naive flip proves it."""
import threading
import time

from common import JOURNAL, SHARDS, conn, container_dsn, log, read_jsonl, write_router_state
from drill_naive import audit
from replicate import capture_lsn, wait_for_lsn
from traffic import Traffic


def flip_replication_streams():
    """Forward subs must go first (or they'd loop reverse-applied rows back);
    the reverse stream must exist before writes resume (or the first writes on
    the shards would predate its slot and be unrecoverable on rollback)."""
    for i, shard in enumerate(SHARDS):
        with conn(shard) as c:
            c.execute(f"DROP SUBSCRIPTION sub_shard{i}")
            c.execute(f"CREATE PUBLICATION pub_back{i} FOR TABLE docs")
    with conn("mono") as mono:
        for i, shard in enumerate(SHARDS):
            mono.execute(
                f"CREATE SUBSCRIPTION sub_back{i} "
                f"CONNECTION '{container_dsn(shard)}' PUBLICATION pub_back{i} "
                f"WITH (copy_data = false)"
            )


def gated_cutover():
    t0 = time.monotonic()
    log.info("gate: pausing writes at the router")
    write_router_state({"authoritative": "mono", "writes_gated": True})
    time.sleep(0.3)  # in-flight acked writes land before the LSN capture

    lsn = capture_lsn()
    log.info("quiesced; captured mono LSN %s — waiting for both shards", lsn)
    wait_for_lsn(lsn)
    log.info("both shards replayed past %s — flipping replication streams", lsn)
    flip_replication_streams()
    log.info("reverse replication armed (shards -> mono) — flipping routing")

    write_router_state({"authoritative": "shards", "writes_gated": False})
    pause = time.monotonic() - t0
    log.info("cutover complete: shards authoritative, writes resumed")
    return pause


def main():
    stop = threading.Event()
    traffic = Traffic()
    thread = threading.Thread(target=traffic.run, args=(stop,), daemon=True)
    thread.start()
    time.sleep(2.0)

    pre_flip = len(read_jsonl(JOURNAL))
    pause = gated_cutover()
    time.sleep(2.0)  # traffic keeps running, now against the shards
    stop.set()
    thread.join()

    records = read_jsonl(JOURNAL)
    acked_to_mono = [r for r in records[:pre_flip] if r["node"] == "mono"]
    damage = audit(acked_to_mono)

    print()
    print(f"write pause during cutover   : {pause * 1000:.0f} ms")
    print(f"acked writes audited on shards: {len(acked_to_mono)}")
    print(f"missing or stale             : {len(damage)}")
    print()
    if damage:
        print(f"❌ CUTOVER LOST DATA: {len(damage)} acked writes damaged")
        raise SystemExit(1)
    print("✅ zero acked writes lost — the LSN gate held the flip until every")
    print("   shard had replayed everything the monolith ever acked.")
    print("   (post-cutover: shards authoritative; inserts stay frozen until")
    print("   `make drill-sequence` fixes the shard sequences)")


if __name__ == "__main__":
    main()
