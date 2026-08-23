"""The gated cutover, Figma/Notion style: gate writes at the router, capture
the monolith's LSN once quiesced, wait until every shard has replayed past it,
flip the routing state atomically, resume. The pause is seconds; the loss is
zero — and the same audit that condemned the naive flip proves it."""
import threading
import time

from common import JOURNAL, log, read_jsonl, write_router_state
from drill_naive import audit
from replicate import capture_lsn, wait_for_lsn
from traffic import Traffic


def gated_cutover():
    t0 = time.monotonic()
    log.info("gate: pausing writes at the router")
    write_router_state({"authoritative": "mono", "writes_gated": True})
    time.sleep(0.3)  # in-flight acked writes land before the LSN capture

    lsn = capture_lsn()
    log.info("quiesced; captured mono LSN %s — waiting for both shards", lsn)
    wait_for_lsn(lsn)
    log.info("both shards replayed past %s — flipping routing", lsn)

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
