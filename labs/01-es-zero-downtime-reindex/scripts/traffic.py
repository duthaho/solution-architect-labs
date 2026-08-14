"""Continuous live write traffic — the thing that makes this problem hard.

Simulates the application writing during the migration:
- ~60% new documents, ~35% updates to existing ones, ~5% soft deletes.
- Every operation goes through WRITE_ALIAS (never a concrete index).
- Every acknowledged op is appended to a journal file (journal.jsonl). The
  journal is our ground truth: verify.py later proves that every acknowledged
  write survived the migration — that is the "no data gap" guarantee.

Production lesson baked in: the writer RETRIES on cluster_block_exception.
The cutover strategy briefly blocks writes (seconds); real-world writers must
tolerate that with retry + backoff. This is exactly how gh-ost / pt-osc
cutovers behave on MySQL too.
"""
import json
import random
import signal
import sys
import time

from elasticsearch import ApiError, TransportError

from common import LAB_DIR, SEED_DOCS, WRITE_ALIAS, es_client, log, now_millis, wait_for_es
from seed import make_doc

JOURNAL = LAB_DIR / "journal.jsonl"
RUNNING = True


def _stop(*_):
    global RUNNING
    RUNNING = False


def write_with_retry(es, doc_id: str, doc: dict, max_wait_s: float = 60.0) -> bool:
    """Index one doc, retrying while writes are blocked. Returns True if acked."""
    deadline = time.time() + max_wait_s
    backoff = 0.2
    while time.time() < deadline:
        try:
            es.index(index=WRITE_ALIAS, id=doc_id, document=doc)
            return True
        except (ApiError, TransportError) as e:
            msg = str(e)
            if "cluster_block_exception" in msg or "blocked" in msg or "429" in msg:
                time.sleep(backoff)
                backoff = min(backoff * 2, 2.0)
                continue
            raise
    return False


def main() -> None:
    signal.signal(signal.SIGTERM, _stop)
    signal.signal(signal.SIGINT, _stop)

    es = es_client(timeout=30)
    wait_for_es(es)
    rng = random.Random()
    next_new_id = SEED_DOCS  # new docs start after the seeded range
    ops = blocked_retries = 0

    log.info("Traffic generator started (journal: %s)", JOURNAL)
    with open(JOURNAL, "a") as journal:
        while RUNNING:
            roll = rng.random()
            if roll < 0.60:
                doc_id, op = f"p-{next_new_id}", "create"
                doc = make_doc(next_new_id, rng)
                next_new_id += 1
            elif roll < 0.95:
                target = rng.randrange(0, SEED_DOCS)
                doc_id, op = f"p-{target}", "update"
                doc = make_doc(target, rng)
            else:
                target = rng.randrange(0, SEED_DOCS)
                doc_id, op = f"p-{target}", "soft_delete"
                doc = make_doc(target, rng)
                doc["is_deleted"] = True
            doc["updated_at"] = now_millis()

            t0 = time.time()
            acked = write_with_retry(es, doc_id, doc)
            wait = time.time() - t0
            if wait > 0.5:
                blocked_retries += 1
                log.warning("Write %s delayed %.2fs (writes blocked during cutover?)", doc_id, wait)
            if not acked:
                log.error("Write %s NOT acked within retry budget — data gap!", doc_id)
                sys.exit(2)

            journal.write(json.dumps({"id": doc_id, "op": op, "updated_at": doc["updated_at"]}) + "\n")
            journal.flush()
            ops += 1
            if ops % 500 == 0:
                log.info("  %d ops journaled (%d delayed by cutover)", ops, blocked_retries)
            time.sleep(rng.uniform(0.005, 0.02))

    log.info("Traffic stopped: %d ops journaled, %d delayed writes, 0 lost", ops, blocked_retries)


if __name__ == "__main__":
    main()
