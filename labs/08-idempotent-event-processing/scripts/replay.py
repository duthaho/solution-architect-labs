"""Drain the DLQ back into the payments topic — the second half of the DLQ
bargain. Quarantining (consumer.py --dlq) buys progress; replay pays the
debt: the parked events still represent real money.

Repair strategy: look the event up in the source of truth (the payments
table) and rebuild a well-formed event from it. That's the general shape of
every real DLQ replay — you fix the DATA (or deploy a consumer that can
parse it), then re-emit.

--times N republishes every DLQ event N times. That is not a bug, it's the
demonstration: replay tooling is exactly the kind of ad-hoc, run-from-a-
terminal code that double-fires (operator reruns it, script crashes halfway,
two people replay at once). It is SAFE here only because the consumer
dedupes in its transaction — drill 2 is what makes drill 4's replay boring.
"""
import argparse
import json

from confluent_kafka import Consumer, KafkaError, Producer

from common import DLQ_TOPIC, KAFKA_BOOTSTRAP, TOPIC, connect_mysql, event_bytes, log, producer_conf


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--times", type=int, default=1,
                    help="republish each event N times (proves replay is dedupe-safe)")
    args = ap.parse_args()

    consumer = Consumer({
        "bootstrap.servers": KAFKA_BOOTSTRAP,
        "group.id": "dlq-replay",
        "enable.auto.commit": False,
        "auto.offset.reset": "earliest",
        "enable.partition.eof": True,   # DLQ is finite: read to the end, stop
    })
    consumer.subscribe([DLQ_TOPIC])
    producer = Producer(producer_conf())
    conn = connect_mysql()

    replayed = skipped = 0
    done = False
    while not done:
        msg = consumer.poll(timeout=10.0)
        if msg is None:
            break  # nothing assigned / empty DLQ
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                done = True
                continue
            raise RuntimeError(msg.error())

        try:
            event_id = json.loads(msg.value()).get("event_id")
        except Exception:
            log.warning("DLQ event at offset %d is not even JSON — leaving it parked",
                        msg.offset())
            skipped += 1
            continue
        with conn.cursor() as cur:
            cur.execute(
                "SELECT account_id, amount_cents FROM payments WHERE event_id = %s",
                (event_id,))
            row = cur.fetchone()
        if row is None:
            log.warning("DLQ event %s has no payment row — not ours to replay, skipping",
                        event_id)
            skipped += 1
            continue
        account_id, amount = row
        for _ in range(args.times):
            producer.produce(TOPIC, key=str(account_id).encode(),
                             value=event_bytes(event_id, account_id, amount))
        replayed += 1
        log.info("Repaired + replayed %s (account %d, $%.2f) x%d",
                 event_id, account_id, amount / 100, args.times)

    producer.flush(30)
    if replayed or skipped:
        consumer.commit(asynchronous=False)
    consumer.close()
    conn.close()
    log.info("Replay done: %d events replayed x%d, %d skipped", replayed, args.times, skipped)


if __name__ == "__main__":
    main()
