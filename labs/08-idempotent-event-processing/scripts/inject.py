"""Inject the poison pill: a real, accepted payment whose event is garbage.

The business half is legitimate — a payment row lands in MySQL like any
other. The wire half is broken: amount_cents goes out as the STRING "NaN"
(a buggy producer deploy, a schema drift, a hand-crafted replay — pick your
incident). JSON happily encodes it, the topic happily stores it, and the
consumer chokes on it at parse time. One bad event, produced once, is now
permanently in the log in front of every event behind it.
"""
import uuid

from confluent_kafka import Producer

from common import TOPIC, connect_mysql, event_bytes, log, producer_conf

PILL_ACCOUNT = 1
PILL_AMOUNT = 42_42  # the true amount, safe in MySQL — the topic gets "NaN"


def main() -> None:
    event_id = str(uuid.uuid4())

    conn = connect_mysql()
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO payments (event_id, account_id, amount_cents) VALUES (%s,%s,%s)",
            (event_id, PILL_ACCOUNT, PILL_AMOUNT))
    conn.close()

    producer = Producer(producer_conf())
    producer.produce(TOPIC, key=str(PILL_ACCOUNT).encode(),
                     value=event_bytes(event_id, PILL_ACCOUNT, "NaN"))
    producer.flush(10)
    log.info("POISON PILL injected: event %s, account %d, true amount $%.2f, "
             "wire amount \"NaN\"", event_id, PILL_ACCOUNT, PILL_AMOUNT / 100)


if __name__ == "__main__":
    main()
