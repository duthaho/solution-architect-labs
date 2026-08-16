"""Create topics + tables + seed accounts. `--reset` wipes state for a fresh
drill without restarting containers (each drill needs a clean ledger).

The payments topic gets 3 partitions, keyed by account_id: all events for an
account land in one partition, consumed in order. Balance addition happens to
be commutative, so this lab would survive reordering — but key discipline is
the habit, and drill-poison needs >1 partition to show a *partition* stalling
while the group is stuck.
"""
import argparse
import subprocess

from common import (
    ACCOUNTS, DB, DLQ_TOPIC, LAB_DIR, PARTITIONS, SEED_BALANCE_CENTS, TOPIC,
    connect_mysql, kafka_cli, log, wait_for_mysql,
)

TABLES = ["accounts", "payments", "processed_events", "outbox"]
GROUPS = ["balance-consumer", "dlq-replay"]


def topic_exists(name: str) -> bool:
    out = kafka_cli("/opt/kafka/bin/kafka-topics.sh",
                    "--bootstrap-server", "localhost:9092", "--list")
    return name in out.split()


def create_topics() -> None:
    for topic, parts in ((TOPIC, PARTITIONS), (DLQ_TOPIC, 1)):
        if topic_exists(topic):
            log.info("Topic %s already exists", topic)
            continue
        kafka_cli("/opt/kafka/bin/kafka-topics.sh",
                  "--bootstrap-server", "localhost:9092", "--create",
                  "--topic", topic, "--partitions", str(parts),
                  "--replication-factor", "1")
        log.info("Created topic %s (%d partition%s)", topic, parts, "s" if parts > 1 else "")


def delete_topics_and_groups() -> None:
    # Deleting a topic does NOT reset committed group offsets — a recreated
    # topic starts at offset 0 while the group still remembers offset N.
    # Delete the groups too, or every drill after the first replays nothing.
    for group in GROUPS:
        subprocess.run(
            ["docker", "exec", "lab08-kafka", "/opt/kafka/bin/kafka-consumer-groups.sh",
             "--bootstrap-server", "localhost:9092", "--delete", "--group", group],
            capture_output=True)  # ok to fail: group may not exist yet
    for topic in (TOPIC, DLQ_TOPIC):
        if topic_exists(topic):
            kafka_cli("/opt/kafka/bin/kafka-topics.sh",
                      "--bootstrap-server", "localhost:9092", "--delete",
                      "--topic", topic)
    log.info("Deleted topics %s + consumer groups %s", [TOPIC, DLQ_TOPIC], GROUPS)


def create_tables(conn) -> None:
    with conn.cursor() as cur:
        cur.execute(f"CREATE DATABASE IF NOT EXISTS {DB}")
        cur.execute(f"USE {DB}")
        # schema.sql is all CREATE TABLE IF NOT EXISTS — safe to rerun.
        # (Strip comment lines first: a ';' inside a comment breaks the split.)
        sql = "\n".join(
            line for line in (LAB_DIR / "sql" / "schema.sql").read_text().splitlines()
            if not line.lstrip().startswith("--"))
        for stmt in sql.split(";"):
            if stmt.strip():
                cur.execute(stmt)
        log.info("Tables ready: %s", TABLES)


def seed_accounts(conn) -> None:
    with conn.cursor() as cur:
        cur.execute(f"USE {DB}")
        cur.execute("SELECT COUNT(*) FROM accounts")
        if cur.fetchone()[0]:
            log.info("Accounts already seeded")
            return
        cur.executemany(
            "INSERT INTO accounts (id, balance_cents) VALUES (%s, %s)",
            [(i, SEED_BALANCE_CENTS) for i in range(1, ACCOUNTS + 1)])
    log.info("Seeded %d accounts at $%.2f each", ACCOUNTS, SEED_BALANCE_CENTS / 100)


def reset(conn) -> None:
    with conn.cursor() as cur:
        cur.execute(f"USE {DB}")
        for table in TABLES:
            cur.execute(f"TRUNCATE TABLE {table}")
    log.info("Truncated %s", TABLES)
    delete_topics_and_groups()


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--reset", action="store_true",
                    help="wipe tables/topics/group offsets, then re-seed (fresh drill)")
    args = ap.parse_args()

    wait_for_mysql()
    conn = connect_mysql(db=None)
    create_tables(conn)
    if args.reset:
        reset(conn)
    seed_accounts(conn)
    create_topics()
    conn.close()
    log.info("Bootstrap complete.")


if __name__ == "__main__":
    main()
