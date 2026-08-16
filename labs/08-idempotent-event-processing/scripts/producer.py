"""The payment service. Two modes, one dual-write bug.

--mode direct   The textbook mistake: write the payment to MySQL, then
                publish the event to Kafka. Two systems, no shared
                transaction. Crash between the two (--crash-at) and the DB
                says "payment accepted" while Kafka never hears about it —
                the GHOST PUBLISH of drill 3. No retry policy fixes this:
                the process that was supposed to retry is dead.

--mode outbox   The fix: payment row + outbox row in ONE MySQL transaction.
                The producer never touches Kafka; relay.py polls the outbox
                and publishes. Crash anywhere and the event is either
                durably queued (relay delivers it) or the transaction rolled
                back (caller sees the failure). Nothing in between exists.

--retry-storm   Simulates application-level publish retries: ~5% of events
                are produce()d twice. Kafka's idempotent producer dedupes
                *transport* retries (same in-flight request), but an app
                calling send() again after a timeout is a brand-new record.
                At-least-once is the floor you build on, not a bug to fix.
"""
import argparse
import os
import random
import sys
import uuid

from confluent_kafka import Producer

from common import ACCOUNTS, TOPIC, connect_mysql, event_bytes, log, producer_conf

DUPE_RATE = 0.05


def make_event(seq: int) -> tuple[str, int, int]:
    # Strictly positive amounts, on purpose: then the audit's drift DIRECTION
    # identifies the failure mode — actual > expected means duplicate
    # effects, actual < expected means events that never got applied.
    amount = random.randint(100, 50_00)  # $1..$50
    return str(uuid.uuid4()), random.randint(1, ACCOUNTS), amount


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--mode", choices=["direct", "outbox"], default="direct")
    ap.add_argument("--count", type=int, default=2000)
    ap.add_argument("--retry-storm", action="store_true",
                    help="duplicate ~5%% of publishes (app-level retries)")
    ap.add_argument("--crash-at", type=int, default=0,
                    help="os._exit(1) while handling the Nth event: after the DB "
                         "write, before the publish (direct) / after the txn (outbox)")
    ap.add_argument("--start-at", type=int, default=1,
                    help="display numbering offset for 'restarted service' runs")
    args = ap.parse_args()

    conn = connect_mysql(autocommit=(args.mode == "direct"))
    producer = Producer(producer_conf()) if args.mode == "direct" else None

    published = duped = 0
    for i in range(1, args.count + 1):
        event_id, account_id, amount = make_event(i)
        seq = args.start_at + i - 1

        if args.mode == "direct":
            # Write 1 of the dual write: the business decision, committed.
            with conn.cursor() as cur:
                cur.execute(
                    "INSERT INTO payments (event_id, account_id, amount_cents) VALUES (%s,%s,%s)",
                    (event_id, account_id, amount))
            if args.crash_at == i:
                log.error("CRASH between DB commit and publish (event #%d, %s, "
                          "account %d, %+d cents) — this payment is now a ghost. "
                          "Bonus damage: produce() is async, so any earlier events "
                          "still sitting unacked in the client buffer die with us too.",
                          seq, event_id, account_id, amount)
                os._exit(1)
            # Write 2 of the dual write: the announcement. If we died one
            # line ago, this never happens and nobody will ever know.
            payload = event_bytes(event_id, account_id, amount)
            producer.produce(TOPIC, key=str(account_id).encode(), value=payload)
            published += 1
            if args.retry_storm and random.random() < DUPE_RATE:
                producer.produce(TOPIC, key=str(account_id).encode(), value=payload)
                published += 1
                duped += 1
        else:
            # ONE transaction, ONE system. The outbox row and the payment
            # commit or vanish together — there is no in-between state.
            with conn.cursor() as cur:
                cur.execute(
                    "INSERT INTO payments (event_id, account_id, amount_cents) VALUES (%s,%s,%s)",
                    (event_id, account_id, amount))
                cur.execute(
                    "INSERT INTO outbox (event_id, account_id, amount_cents) VALUES (%s,%s,%s)",
                    (event_id, account_id, amount))
            conn.commit()
            if args.crash_at == i:
                log.error("CRASH right after txn commit (event #%d, %s) — harmless: "
                          "payment AND outbox row are durable, relay will publish it",
                          seq, event_id)
                os._exit(1)

    if producer is not None:
        remaining = producer.flush(30)
        if remaining:
            log.error("%d events still unflushed after 30s", remaining)
            sys.exit(1)

    if args.mode == "direct":
        log.info("Produced %d payments -> %d publishes (%d app-retry duplicates)",
                 args.count, published, duped)
    else:
        log.info("Wrote %d payments + outbox rows (0 publishes — that's relay.py's job)",
                 args.count)


if __name__ == "__main__":
    main()
