"""The projector: Kafka consumer that projects CDC events onto Elasticsearch.

This is the part you write yourself even in production shops that run
Debezium off the shelf (or you run Confluent's ES sink connector — after this
lab you'll know exactly what it does). Everything that makes CDC consumption
correct lives in ~100 lines:

AT-LEAST-ONCE + IDEMPOTENT = "EXACTLY-ONCE"
  Kafka's default is auto-commit: offsets are committed on a timer,
  *independently* of whether your side effect happened — crash at the wrong
  moment and you either lose events (committed but not applied) or redo them.
  This consumer disables auto-commit and commits offsets only AFTER the ES
  bulk write is acknowledged. Crash anywhere: events replay. Replay is
  harmless because projection is idempotent — doc _id = row PK, whole-doc
  upsert, deletes ignore 404. Duplicate-tolerant writes + commit-after-write
  is the entire "exactly-once" illusion (lab 05 makes this the headline).

ORDERING
  Debezium keys events by PK, so a row's events all live in one partition,
  consumed in order. Cross-row order across partitions is NOT preserved —
  and a projection doesn't need it. What would break it: repartitioning by
  another field, or a fan-out step that round-robins. README §3.3.

THE ENVELOPE
  op 'r' (snapshot read) / 'c' / 'u'  -> index(after)   [all the same: upsert]
  op 'd'                              -> delete(before.id)
  value == null (tombstone)           -> skip (it's for Kafka log compaction,
                                        not for consumers)
"""
import json
import signal
import time

from confluent_kafka import Consumer, KafkaError
from elasticsearch.helpers import bulk

from common import CONSUMER_GROUP, ES_INDEX, KAFKA_BOOTSTRAP, TOPIC, es_client, log, row_to_doc, wait_for_es

BATCH_MAX = 1000
BATCH_WAIT_S = 0.3
RUNNING = True


def _stop(*_):
    global RUNNING
    RUNNING = False


def to_action(msg) -> dict | None:
    if msg.value() is None:
        return None  # tombstone
    envelope = json.loads(msg.value())
    op = envelope.get("op")
    if op in ("r", "c", "u"):
        doc = row_to_doc(envelope["after"])
        return {"_op_type": "index", "_index": ES_INDEX, "_id": doc["id"], "_source": doc}
    if op == "d":
        return {"_op_type": "delete", "_index": ES_INDEX, "_id": envelope["before"]["id"]}
    return None  # schema/heartbeat events, if any slip through


def flush(es, consumer, actions: list[dict]) -> None:
    if actions:
        # raise_on_error=False + manual scan: a delete hitting 404 is a
        # legitimate replay artifact (we already deleted it last time), not a
        # failure. Anything else aborts BEFORE the commit — at-least-once.
        _, errors = bulk(es, actions, raise_on_error=False, stats_only=False)
        real = [e for e in errors
                if e.get("delete", {}).get("status") != 404]
        if real:
            raise RuntimeError(f"ES bulk failures (offsets NOT committed): {real[:3]}")
    consumer.commit(asynchronous=False)  # the contract: commit only after ES acked


def main() -> None:
    signal.signal(signal.SIGTERM, _stop)
    signal.signal(signal.SIGINT, _stop)

    es = es_client()
    wait_for_es(es)
    consumer = Consumer({
        "bootstrap.servers": KAFKA_BOOTSTRAP,
        "group.id": CONSUMER_GROUP,
        "enable.auto.commit": False,          # THE line this lab is about
        "auto.offset.reset": "earliest",      # first run: start from the snapshot
        "partition.assignment.strategy": "cooperative-sticky",
    })
    consumer.subscribe([TOPIC])
    log.info("Projector started: %s -> ES index %s (group %s, manual commits)",
             TOPIC, ES_INDEX, CONSUMER_GROUP)

    actions: list[dict] = []
    batch_started = time.time()
    total = upserts = deletes = 0
    t_log = time.time()

    while RUNNING:
        msgs = consumer.consume(num_messages=BATCH_MAX, timeout=0.1)
        for msg in msgs:
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                raise RuntimeError(msg.error())
            action = to_action(msg)
            if action is not None:
                actions.append(action)
                if action["_op_type"] == "delete":
                    deletes += 1
                else:
                    upserts += 1
            total += 1

        if actions and (len(actions) >= BATCH_MAX or time.time() - batch_started > BATCH_WAIT_S):
            flush(es, consumer, actions)
            actions = []
            batch_started = time.time()

        if time.time() - t_log > 5 and total:
            log.info("  projected %d events (%d upserts, %d deletes)", total, upserts, deletes)
            t_log = time.time()

    if actions:  # empty commit would raise _NO_OFFSET on a clean shutdown
        flush(es, consumer, actions)
    consumer.close()
    log.info("Projector stopped: %d events (%d upserts, %d deletes)", total, upserts, deletes)


if __name__ == "__main__":
    main()
