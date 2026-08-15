"""CDC cache invalidator: consumes Debezium change events for lab07.products
and DELETEs the corresponding cache keys.

This is the whole point of the cdc strategy: the application's write path
does NOTHING about the cache. Invalidation is driven by what the binlog says
actually committed — so it also covers writes from cron jobs, migrations,
DBAs in a mysql shell, and the app code path someone forgot to instrument.

Delete, not re-fill from the event payload: a delete is idempotent and
order-insensitive (worst case: an extra cache miss), while a re-fill would
have to prove the event is newer than what a concurrent reader just SET —
that proof needs versioning anyway, so keep the consumer trivial.

Per event we journal lag = now - source.ts_ms (commit time in the binlog).
That number IS the staleness bound of the cdc strategy — when this process
dies, the auditor watches the bound grow (drill-cdc kills us on purpose).
Offsets are committed after processing: restart => replay a little, delete
keys twice, harmless (idempotent). At-least-once is the right guarantee here.
"""
import json
import signal

from confluent_kafka import Consumer

from common import KAFKA_BOOTSTRAP, LAG_JOURNAL, connect_redis, journal, log, now_ms

TOPIC = "lab07.lab07.products"
running = True


def main() -> None:
    def stop(*_):
        global running
        running = False
    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)

    consumer = Consumer({
        "bootstrap.servers": KAFKA_BOOTSTRAP,
        "group.id": "lab07-invalidator",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": True,          # after poll: at-least-once
    })
    consumer.subscribe([TOPIC])
    r = connect_redis()
    log.info("invalidator: consuming %s -> DEL p:{id}", TOPIC)

    n = 0
    while running:
        msg = consumer.poll(0.5)
        if msg is None or msg.error():
            continue
        if msg.value() is None:              # tombstone (disabled, but be safe)
            continue
        event = json.loads(msg.value())
        payload = event.get("payload", event)  # tolerate schemas.enable variance
        if payload.get("op") not in ("c", "u", "d"):
            continue                          # snapshot/schema events: nothing to invalidate
        row = payload.get("after") or payload.get("before") or {}
        pid = row.get("id")
        if pid is None:
            continue
        r.delete(f"p:{pid}")
        lag_ms = max(0, now_ms() - payload["source"]["ts_ms"])
        journal(LAG_JOURNAL, {"ts": now_ms(), "id": pid, "op": payload["op"],
                              "lag_ms": lag_ms})
        n += 1
        if n % 200 == 0:
            log.info("invalidated %d keys (last lag %dms)", n, lag_ms)

    consumer.close()
    r.close()
    log.info("invalidator stopped after %d invalidations", n)


if __name__ == "__main__":
    main()
