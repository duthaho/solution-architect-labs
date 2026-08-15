"""Per-shard chunked backfill: copy every row a shard owns from mono onto it.

Safety rests on three legs (README §3.2):

  1. MUST run under double-write (checked): every row inserted or updated
     after the flip reaches its shard as a full-state upsert, so the backfill
     only has to deliver rows that PREdate double-write.
  2. INSERT IGNORE on the shard: the backfill NEVER overwrites. If a row it
     is carrying was double-written meanwhile, the shard copy is fresher (or
     equal) and the stale mono snapshot is dropped on the floor. "Double-write
     always wins, backfill never fights" — lab 02's rule, one level up.
  3. Chunk boundaries walk the real PK (user_id, seq) with a probe query, not
     id arithmetic — and resume from backfill_state.json, which is written
     AFTER each chunk commits. A crash between commit and journal replays one
     chunk, which INSERT IGNORE makes free. That is the entire crash story:
     redo is safe, so exactly-once bookkeeping is unnecessary.

Unlike lab 02, the copy is SELECT-into-Python-then-INSERT: mono and the shard
are different servers, so there is no server-side INSERT..SELECT to lean on —
which is also why the throttle knob matters more here (two networks pay).

--repair replays repair_queue.jsonl entries targeting the shards (the queued
partial writes from drill 5) before backfilling.
"""
import argparse
import json
import signal
import sys
import time

from common import (
    BACKFILL_STATE,
    SHARDS,
    TABLE,
    connect,
    log,
    read_mode,
    replay_repair_queue,
    shard_filter_sql,
    wait_for_node,
)

CHUNK = 2000
COLS = "user_id, seq, status, amount, note, updated_at"


def load_state() -> dict:
    if BACKFILL_STATE.exists():
        return json.loads(BACKFILL_STATE.read_text())
    return {}


def save_state(state: dict) -> None:
    tmp = BACKFILL_STATE.with_suffix(".json.tmp")
    tmp.write_text(json.dumps(state, indent=2))
    tmp.rename(BACKFILL_STATE)


def backfill_shard(shard: str, state: dict, sleep_ms: int) -> None:
    st = state.setdefault(shard, {"last_user": 0, "last_seq": 0, "chunks": 0, "rows": 0,
                                  "done": False})
    if st["done"]:
        log.info("%s: already complete (%d rows in %d chunks) — nothing to do",
                 shard, st["rows"], st["chunks"])
        return
    if st["chunks"]:
        log.info("%s: RESUMING at (user_id, seq) > (%d, %d) — %d rows in %d chunks already copied",
                 shard, st["last_user"], st["last_seq"], st["rows"], st["chunks"])

    mono = connect("mono")
    dst = connect(shard)
    owns = shard_filter_sql(shard)
    t0 = time.time()
    t_log = t0
    with mono.cursor() as src_cur, dst.cursor() as dst_cur:
        while True:
            # One chunk: the next CHUNK owned rows in PK order after the cursor.
            src_cur.execute(
                f"SELECT {COLS} FROM {TABLE} "
                f"WHERE {owns} AND (user_id, seq) > (%s, %s) "
                f"ORDER BY user_id, seq LIMIT %s",
                (st["last_user"], st["last_seq"], CHUNK))
            rows = src_cur.fetchall()
            if not rows:
                break
            dst_cur.executemany(
                f"INSERT IGNORE INTO {TABLE} ({COLS}) VALUES (%s, %s, %s, %s, %s, %s)",
                rows)
            # State is journaled only AFTER the chunk is committed on the shard.
            st["last_user"], st["last_seq"] = int(rows[-1][0]), int(rows[-1][1])
            st["chunks"] += 1
            st["rows"] += len(rows)
            save_state(state)

            if time.time() - t_log > 5:
                log.info("  %s: %d rows in %d chunks (%.0f rows/s), cursor=(%d,%d)",
                         shard, st["rows"], st["chunks"], st["rows"] / (time.time() - t0),
                         st["last_user"], st["last_seq"])
                t_log = time.time()
            if sleep_ms:
                time.sleep(sleep_ms / 1000)

    st["done"] = True
    save_state(state)
    mono.close()
    dst.close()
    log.info("%s: backfill COMPLETE — %d rows in %d chunks, %.1fs",
             shard, st["rows"], st["chunks"], time.time() - t0)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--shard", choices=SHARDS, help="backfill one shard (default: all)")
    parser.add_argument("--chunk-sleep-ms", type=int, default=0,
                        help="throttle between chunks (production: start at 50)")
    parser.add_argument("--repair", action="store_true",
                        help="first replay repair_queue.jsonl entries targeting the shards")
    parser.add_argument("--force", action="store_true",
                        help="skip the double-write mode check (you are lying to the safety model)")
    args = parser.parse_args()

    signal.signal(signal.SIGTERM, lambda *_: sys.exit(1))

    mode = read_mode()
    if mode not in ("double-write", "shadow-read") and not args.force:
        raise SystemExit(
            f"router mode is '{mode}' — backfill is only safe under double-write "
            "(rows written during the copy would never reach their shard). "
            "Run `make double-write` first, or --force to watch it go wrong.")

    for node in ["mono"] + SHARDS:
        wait_for_node(node, timeout_s=30)

    if args.repair:
        replay_repair_queue(targets=set(SHARDS))

    state = load_state()
    for shard in ([args.shard] if args.shard else SHARDS):
        backfill_shard(shard, state, args.chunk_sleep_ms)
    log.info("Backfill done. Next: python scripts/verify.py")


if __name__ == "__main__":
    main()
