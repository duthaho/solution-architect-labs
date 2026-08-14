"""Create the MySQL source table, the ES target index, and the Kafka topic.

The topic is created explicitly with 3 partitions instead of letting the
broker auto-create it with 1: partitioning is where CDC ordering lives
(README §3.3), and a single-partition topic would hide the whole subject.
Debezium keys every event by the row's primary key, so all events for a
given row always land in the same partition — total order *per row*, which
is exactly the guarantee a projection needs (and all it needs).
"""
import subprocess

from common import (
    DB,
    ES_INDEX,
    LAB_DIR,
    TABLE,
    TOPIC,
    connect_mysql,
    es_client,
    load_mapping,
    log,
    wait_for_es,
    wait_for_mysql,
)

PARTITIONS = 3


def main() -> None:
    wait_for_mysql()
    conn = connect_mysql(db=None)
    with conn.cursor() as cur:
        cur.execute(f"CREATE DATABASE IF NOT EXISTS {DB}")
        cur.execute(f"USE {DB}")
        cur.execute(
            "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema=%s AND table_name=%s",
            (DB, TABLE))
        if cur.fetchone()[0]:
            raise SystemExit(f"Table {TABLE} already exists — run `make clean` for a fresh start")
        cur.execute((LAB_DIR / "sql" / "schema.sql").read_text())
    conn.close()
    log.info("Created %s.%s (source of truth)", DB, TABLE)

    es = es_client()
    wait_for_es(es)
    if es.indices.exists(index=ES_INDEX):
        raise SystemExit(f"Index {ES_INDEX} already exists — run `make clean` for a fresh start")
    body = load_mapping()
    es.indices.create(index=ES_INDEX, settings=body["settings"], mappings=body["mappings"])
    log.info("Created ES index %s (dynamic:strict — surprise fields fail loudly)", ES_INDEX)

    subprocess.run(
        ["docker", "exec", "lab03-kafka", "/opt/kafka/bin/kafka-topics.sh",
         "--bootstrap-server", "localhost:9092", "--create",
         "--topic", TOPIC, "--partitions", str(PARTITIONS), "--replication-factor", "1"],
        check=True, capture_output=True)
    log.info("Created topic %s with %d partitions (keyed by PK)", TOPIC, PARTITIONS)
    log.info("Bootstrap complete. Next: make seed")


if __name__ == "__main__":
    main()
