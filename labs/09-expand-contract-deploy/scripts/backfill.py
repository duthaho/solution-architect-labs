"""MIGRATE: fill first/last on every row the dual-write hasn't touched.
Lab 02's backfill pattern in miniature: chunked by PK, throttled, resumable.

The concurrency argument, in one sentence: until reads flip, `name` is the
AUTHORITATIVE shape — so every chunk is ONE UPDATE that converges first/last
toward the split of the row's CURRENT name, guarded to touch only rows where
they're missing OR disagree. A fresh v1.5 dual-write is consistent by
construction (predicate false, untouched); a rerun or a replayed chunk is a
no-op. Same idea as lab 06's "the authoritative copy wins, repairs converge".

Why "disagree" and not just "IS NULL": the rolling window v1 -> v1.5 can
interleave, on ONE row, a v1.5 dual-write then a v1 name-only write — leaving
first/last populated but STALE. An IS-NULL-guarded backfill skips that row
forever. This lab's verifier caught exactly that (1 row in ~5k ops) on the
first full run; the disagreement predicate is the fix, and shapes-verify
proves it.

State (backfill_state.json, atomic rename) records the last completed chunk
boundary. SIGKILL it mid-run and rerun: it prints RESUMING and continues —
that's drill-crash-backfill's whole assertion.
"""
import argparse
import json
import time

import common as c


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--chunk-size", type=int, default=5000)
    ap.add_argument("--chunk-sleep-ms", type=int, default=5,
                    help="throttle: sleep between chunks (production: adaptive, "
                         "watching replica lag / p99)")
    args = ap.parse_args()

    assert {"first_name", "last_name"} <= c.columns(), "run expand.py first"

    conn = c.connect()
    with conn.cursor() as cur:
        cur.execute(f"SELECT COALESCE(MAX(id), 0) FROM {c.TABLE}")
        max_id = int(cur.fetchone()[0])  # COALESCE promotes to Decimal

    last_id = 0
    if c.BACKFILL_STATE.exists():
        state = json.loads(c.BACKFILL_STATE.read_text())
        last_id = state["last_id"]
        c.log(f"RESUMING backfill from id {last_id} (journal: {c.BACKFILL_STATE.name})")

    t0, filled = time.time(), 0
    needs_fix = (f"(first_name IS NULL OR first_name <> {c.SQL_FIRST} "
                 f"OR last_name <> {c.SQL_LAST})")
    sql = (f"UPDATE {c.TABLE} SET first_name = {c.SQL_FIRST}, last_name = {c.SQL_LAST} "
           f"WHERE id > %s AND id <= %s AND name IS NOT NULL AND {needs_fix}")
    while last_id < max_id:
        hi = min(last_id + args.chunk_size, max_id)
        with conn.cursor() as cur:
            cur.execute(sql, (last_id, hi))
            filled += cur.rowcount
        last_id = hi
        tmp = c.BACKFILL_STATE.with_suffix(".json.tmp")
        tmp.write_text(json.dumps({"last_id": last_id, "max_id": max_id}) + "\n")
        tmp.rename(c.BACKFILL_STATE)
        time.sleep(args.chunk_sleep_ms / 1000)

    # Sweep the stragglers: rows a lingering v1 pod wrote old-shape (or wrote
    # OVER a dual-write) inside the already-scanned range during the roll.
    with conn.cursor() as cur:
        cur.execute(f"UPDATE {c.TABLE} SET first_name = {c.SQL_FIRST}, "
                    f"last_name = {c.SQL_LAST} WHERE name IS NOT NULL AND {needs_fix}")
        stragglers = cur.rowcount
    c.log(f"backfill done: {filled} rows filled in chunks, {stragglers} stragglers "
          f"swept, up to id {max_id}, {time.time() - t0:.1f}s")


if __name__ == "__main__":
    main()
