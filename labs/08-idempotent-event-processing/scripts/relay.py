"""The outbox relay: poll unpublished outbox rows, publish, mark published.

The relay completes the outbox pattern (producer.py --mode outbox). Note
what it does NOT try to be: exactly-once. It publishes THEN marks — crash
between the two and the row is republished on restart. That's a deliberate
choice, and the only sane one:

  mark-then-publish  -> crash loses events   (at-most-once: unacceptable)
  publish-then-mark  -> crash duplicates     (at-least-once: fine, the
                        consumer's dedupe absorbs it)

This is Debezium's outbox router / every homegrown relay in one loop. The
whole system leans on one invariant: every component may duplicate, exactly
one component (the consumer's txn) deduplicates.
"""
import signal
import sys
import time

from confluent_kafka import Producer

from common import TOPIC, connect_mysql, event_bytes, log, producer_conf

BATCH = 500
POLL_S = 0.5
RUNNING = True


def _stop(*_):
    global RUNNING
    RUNNING = False


def main() -> None:
    signal.signal(signal.SIGTERM, _stop)
    signal.signal(signal.SIGINT, _stop)

    conn = connect_mysql(autocommit=True)
    producer = Producer(producer_conf())
    total = 0
    log.info("Relay started: outbox -> %s (publish-then-mark, at-least-once)", TOPIC)

    while RUNNING:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT id, event_id, account_id, amount_cents FROM outbox "
                "WHERE published_at IS NULL ORDER BY id LIMIT %s", (BATCH,))
            rows = cur.fetchall()
        if not rows:
            time.sleep(POLL_S)
            continue

        for _, event_id, account_id, amount in rows:
            producer.produce(TOPIC, key=str(account_id).encode(),
                             value=event_bytes(event_id, account_id, amount))
        if producer.flush(30):
            log.error("Broker did not ack all publishes; not marking, will retry")
            continue
        # Only now — after the broker acked — do rows get marked. A crash on
        # this very line means republish on restart: duplicates, not loss.
        ids = [r[0] for r in rows]
        with conn.cursor() as cur:
            cur.execute(
                f"UPDATE outbox SET published_at = NOW(3) WHERE id IN ({','.join(['%s'] * len(ids))})",
                ids)
        total += len(rows)
        log.info("Relayed %d rows (total %d)", len(rows), total)

    log.info("Relay stopped after %d rows", total)
    sys.exit(0)


if __name__ == "__main__":
    main()
