"""The consumer — the star of the lab. Both modes use IDENTICAL offset
discipline (auto-commit off, manual commit after the batch). The only
difference is inside apply(). That is the whole point:

  Kafka redelivery is at-least-once NO MATTER how carefully you commit
  offsets — crash after applying but before committing (exactly what the
  --crash-every hook forces) and the batch comes back. Offset discipline
  bounds the redelivery window; it cannot close it. Correctness has to live
  where the effect lives: in the database transaction.

--mode naive        UPDATE balance and hope. Every redelivered or
                    producer-duplicated event lands twice. `balance += x`
                    twice is not an error — it's silent corruption that only
                    an audit will ever find.

--mode idempotent   INSERT event_id INTO processed_events + UPDATE balance
                    in ONE transaction. Redelivery hits the primary key,
                    rowcount says "seen it", skip. The dedupe check and the
                    effect commit atomically — which is why this works and a
                    Redis SETNX check-then-act never can (README §4).

--dlq               Malformed events go to payments.dlq instead of crashing
                    the consumer. Without it, one poison pill parks the
                    whole group forever (drill 4).

--crash-every N     os._exit(1) after every Nth processed message — after
                    the effect, before the offset commit. The worst possible
                    moment, on purpose, deterministically.
"""
import argparse
import json
import os
import time

from confluent_kafka import Consumer, KafkaError, Producer

from common import CONSUMER_GROUP, DLQ_TOPIC, KAFKA_BOOTSTRAP, TOPIC, connect_mysql, log, producer_conf


class PoisonPill(Exception):
    pass


def parse(raw: bytes) -> tuple[str, int, int]:
    """Deserialize + validate. Anything that raises here is a *permanent*
    failure — retrying a parse error forever is how partitions die. Contrast
    with a DB deadlock or timeout, which is transient: those must crash the
    consumer (and be retried), never be DLQ'd."""
    ev = json.loads(raw)
    event_id, account_id = ev["event_id"], ev["account_id"]
    if not isinstance(ev["amount_cents"], int):
        raise ValueError(f"amount_cents is not an integer: {ev['amount_cents']!r}")
    return event_id, int(account_id), ev["amount_cents"]


def apply_naive(conn, event_id: str, account_id: int, amount: int) -> bool:
    with conn.cursor() as cur:
        cur.execute("UPDATE accounts SET balance_cents = balance_cents + %s WHERE id = %s",
                    (amount, account_id))
    conn.commit()
    return True


def apply_idempotent(conn, event_id: str, account_id: int, amount: int) -> bool:
    """Returns False when the event was already processed. INSERT IGNORE +
    rowcount: 1 means we own this event, 0 means a previous delivery already
    committed it. Both statements ride one transaction — if the process dies
    after this commit, redelivery is a no-op; if it dies before, nothing
    happened at all. There is no third state."""
    with conn.cursor() as cur:
        cur.execute("INSERT IGNORE INTO processed_events (event_id) VALUES (%s)", (event_id,))
        if cur.rowcount == 0:
            conn.rollback()  # nothing to undo; just close the txn
            return False
        cur.execute("UPDATE accounts SET balance_cents = balance_cents + %s WHERE id = %s",
                    (amount, account_id))
    conn.commit()
    return True


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--mode", choices=["naive", "idempotent"], required=True)
    ap.add_argument("--dlq", action="store_true")
    ap.add_argument("--crash-every", type=int, default=0)
    ap.add_argument("--idle-exit", type=float, default=10.0,
                    help="exit 0 after this many seconds without messages (drained)")
    args = ap.parse_args()

    conn = connect_mysql(autocommit=False)
    apply = apply_naive if args.mode == "naive" else apply_idempotent
    dlq_producer = Producer(producer_conf()) if args.dlq else None

    consumer = Consumer({
        "bootstrap.servers": KAFKA_BOOTSTRAP,
        "group.id": CONSUMER_GROUP,
        "enable.auto.commit": False,       # offsets move only when WE say so
        "auto.offset.reset": "earliest",
        # A SIGKILL'd consumer never leaves the group: the broker keeps its
        # partitions assigned until session.timeout.ms expires. That timeout
        # IS your redelivery latency after a hard crash — 45s by default,
        # 6s here so the chaos drills turn around quickly.
        "session.timeout.ms": 6000,
        "heartbeat.interval.ms": 2000,
    })

    processed = applied = skipped = quarantined = 0
    dirty = False              # do we hold uncommitted offsets?
    last_msg = time.time()

    def on_assign(_c, partitions):
        # Idle-exit must not start ticking while we wait out a rebalance
        # (e.g. waiting for a crashed predecessor's session to expire).
        nonlocal last_msg
        last_msg = time.time()
        log.info("Assigned %s", [f"{p.topic}[{p.partition}]" for p in partitions])

    consumer.subscribe([TOPIC], on_assign=on_assign)
    log.info("Consumer started: mode=%s dlq=%s crash-every=%s",
             args.mode, args.dlq, args.crash_every or "off")

    while True:
        msgs = consumer.consume(num_messages=100, timeout=0.5)
        for msg in msgs:
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                raise RuntimeError(msg.error())
            last_msg = time.time()
            processed += 1
            try:
                event_id, account_id, amount = parse(msg.value())
            except Exception as exc:
                if dlq_producer is None:
                    log.error("POISON PILL at %s[%d]@%d: %s — no DLQ, crashing "
                              "(and I will crash again on restart, forever)",
                              msg.topic(), msg.partition(), msg.offset(), exc)
                    raise PoisonPill(str(exc)) from exc
                # Quarantine, don't block: park the raw bytes in the DLQ and
                # move on. Progress for the many beats ordering for the one —
                # and replay.py can bring it back once it's fixed.
                dlq_producer.produce(DLQ_TOPIC, key=msg.key(), value=msg.value())
                dlq_producer.flush(10)
                quarantined += 1
                dirty = True
                log.warning("Quarantined poison event from %s[%d]@%d -> %s (%s)",
                            msg.topic(), msg.partition(), msg.offset(), DLQ_TOPIC, exc)
                continue

            if apply(conn, event_id, account_id, amount):
                applied += 1
            else:
                skipped += 1
            dirty = True

            if args.crash_every and processed % args.crash_every == 0:
                # Effect committed, offsets NOT. The next consumer to own
                # these partitions re-reads everything since the last batch
                # commit. naive double-applies it; idempotent shrugs.
                log.error("CRASH HOOK: os._exit(1) after %d processed "
                          "(offsets uncommitted -> redelivery incoming)", processed)
                os._exit(1)

        if dirty:
            consumer.commit(asynchronous=False)  # only AFTER effects are in the DB
            dirty = False

        if time.time() - last_msg > args.idle_exit:
            break

    consumer.close()
    conn.close()
    log.info("Drained. processed=%d applied=%d dedupe-skipped=%d quarantined=%d",
             processed, applied, skipped, quarantined)


if __name__ == "__main__":
    main()
