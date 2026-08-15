"""The journaling writer — the application's side of the durability contract.

One single-threaded loop inserting monotonically increasing `seq` values through
router.json. The journal (journal.jsonl) records what the DATABASE PROMISED:

    {"type": "acked",     "seq": N, "ts": ms}   commit returned OK — MySQL told
                                                the app this row is durable.
    {"type": "ambiguous", "seq": N, "ts": ms}   the connection died DURING the
                                                commit — we sent it, got no
                                                answer. Neither acked nor lost.

verify.py later diffs "acked" against what actually survived the failover.
That diff — acked by the primary but absent on the new primary — is the RPO,
measured instead of hand-waved.

Retry subtlety worth reading twice: after a reconnect we retry the SAME seq.
If the ambiguous commit actually landed before the crash, the retry hits the
UNIQUE(seq) key -> duplicate-key error -> we now KNOW it is durable and journal
it as acked. This is why the schema has that unique key: an idempotency key
turns "ambiguous" into an answerable question.
"""
import argparse
import signal
import sys
import time

import pymysql

from common import JOURNAL, connect, current_primary, log, now_ms, query_one

DUP_ENTRY = 1062
running = True


def stop(_sig, _frm):
    global running
    running = False


def journal_line(f, typ: str, seq: int) -> None:
    f.write(f'{{"type": "{typ}", "seq": {seq}, "ts": {now_ms()}}}\n')
    f.flush()


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--sleep-ms", type=float, default=5.0,
                    help="pause between writes (throughput knob)")
    ap.add_argument("--node", default=None,
                    help="bypass the router and write straight at this node "
                         "(used by the zombie drill's stale writer)")
    ap.add_argument("--max-rows", type=int, default=0, help="stop after N acked rows (0 = run forever)")
    ap.add_argument("--tag", default="traffic", help="payload prefix")
    ap.add_argument("--no-journal", action="store_true",
                    help="don't journal (the zombie drill's stale writer must not "
                         "pollute the durability ground truth)")
    args = ap.parse_args()

    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)

    def target_node() -> str:
        return args.node if args.node else current_primary()

    conn = None
    acked = 0
    seq = None
    latencies_ms: list[float] = []
    f = open(JOURNAL, "a") if not args.no_journal else open("/dev/null", "w")
    while running:
        try:
            if conn is None:
                node = target_node()
                conn = connect(node, timeout_s=1.5)
                log.info("connected to %s%s", node, "" if args.node else " (current primary per router)")
                if seq is None:
                    seq = query_one(conn, "SELECT COALESCE(MAX(seq),0) AS m FROM events")["m"] + 1
            t0 = time.monotonic()
            with conn.cursor() as cur:
                cur.execute("INSERT INTO events (seq, payload) VALUES (%s, %s)",
                            (seq, f"{args.tag}-{seq}"))
            latencies_ms.append((time.monotonic() - t0) * 1000.0)
            journal_line(f, "acked", seq)
            acked += 1
            seq += 1
            if args.max_rows and acked >= args.max_rows:
                break
            if args.sleep_ms:
                time.sleep(args.sleep_ms / 1000.0)
        except pymysql.err.IntegrityError as e:
            if e.args[0] == DUP_ENTRY:
                # The ambiguous commit from before the crash actually landed.
                log.info("seq %d: duplicate key on retry -> it WAS durable; acked", seq)
                journal_line(f, "acked", seq)
                acked += 1
                seq += 1
            else:
                raise
        except pymysql.err.OperationalError as e:
            # Connection-level failure. If it happened mid-commit, the outcome
            # is unknowable from here: journal it as ambiguous, retry same seq.
            if conn is not None and seq is not None:
                journal_line(f, "ambiguous", seq)
                log.warning("connection lost around seq %d (%s); reconnecting via router", seq, e.args[:1])
            conn = None
            time.sleep(0.3)
        except pymysql.err.InternalError as e:
            # e.g. 1290 super_read_only: we connected to a node that is not
            # (or no longer) writable. Back off, re-read the router.
            log.warning("write rejected (%s); re-resolving primary", e.args[:1])
            try:
                conn.close()
            except Exception:
                pass
            conn = None
            time.sleep(0.3)

    if latencies_ms:
        lat = sorted(latencies_ms)
        avg = sum(lat) / len(lat)
        p95 = lat[int(len(lat) * 0.95)]
        if not args.no_journal:
            f.write(f'{{"type": "stats", "avg_commit_ms": {avg:.2f}, "p95_commit_ms": {p95:.2f}}}\n')
        log.info("commit latency: avg %.2fms, p95 %.2fms over %d commits", avg, p95, len(lat))
    f.close()
    log.info("writer done: %d rows acked, last seq %s", acked, seq)


if __name__ == "__main__":
    sys.exit(main())
