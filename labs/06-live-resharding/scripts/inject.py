"""Corrupt a shard on purpose, then repair it — never trust a verifier you
haven't watched fail.

    inject.py                     corrupt one owned row on shard0 (amount += 1000)
    inject.py --action delete     delete one owned row instead (count-level gap)
    inject.py --shard shard1      pick the victim shard
    inject.py --repair            restore the injected row from mono

The victim is remembered in inject_state.json so --repair can restore exactly
what was broken. Corruption changes VALUES but not counts — that is the point:
it proves the checksum in verify.py check 1 earns its keep, because a
count-only reconciliation (what most teams ship first) calls this state fine.
"""
import argparse
import json

from common import INJECT_STATE, SHARDS, TABLE, UPSERT, connect, log, shard_filter_sql


def inject(shard: str, action: str) -> None:
    conn = connect(shard)
    with conn.cursor() as cur:
        cur.execute(
            f"SELECT user_id, seq, amount FROM {TABLE} "
            f"WHERE {shard_filter_sql(shard)} ORDER BY RAND() LIMIT 1")
        row = cur.fetchone()
        if row is None:
            raise SystemExit(f"{shard} is empty — run the backfill first")
        user_id, seq, amount = int(row[0]), int(row[1]), float(row[2])
        if action == "corrupt":
            cur.execute(
                f"UPDATE {TABLE} SET amount = amount + 1000 WHERE user_id=%s AND seq=%s",
                (user_id, seq))
        else:
            cur.execute(f"DELETE FROM {TABLE} WHERE user_id=%s AND seq=%s", (user_id, seq))
    conn.close()

    INJECT_STATE.write_text(json.dumps(
        {"shard": shard, "user_id": user_id, "seq": seq, "action": action}, indent=2))
    log.info("INJECTED %s on %s: row (%d,%d) amount was %.2f — same row count, different bytes"
             if action == "corrupt" else
             "INJECTED %s on %s: row (%d,%d) amount was %.2f — one acked row gone",
             action, shard, user_id, seq, amount)
    log.info("now run verify.py and watch check 1 catch it; then inject.py --repair")


def repair() -> None:
    if not INJECT_STATE.exists():
        raise SystemExit("nothing to repair (no inject_state.json)")
    st = json.loads(INJECT_STATE.read_text())
    mono = connect("mono")
    with mono.cursor() as cur:
        cur.execute(
            f"SELECT user_id, seq, status, amount, note FROM {TABLE} WHERE user_id=%s AND seq=%s",
            (st["user_id"], st["seq"]))
        row = cur.fetchone()
    mono.close()
    if row is None:
        raise SystemExit(f"row ({st['user_id']},{st['seq']}) not on mono — cannot repair from source")

    conn = connect(st["shard"])
    with conn.cursor() as cur:
        cur.execute(UPSERT, tuple(row))
    conn.close()
    INJECT_STATE.unlink()
    log.info("REPAIRED (%d,%d) on %s from mono (full-state upsert — the same primitive "
             "the repair queue uses)", st["user_id"], st["seq"], st["shard"])


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--shard", choices=SHARDS, default="shard0")
    parser.add_argument("--action", choices=["corrupt", "delete"], default="corrupt")
    parser.add_argument("--repair", action="store_true")
    args = parser.parse_args()
    if args.repair:
        repair()
    else:
        inject(args.shard, args.action)


if __name__ == "__main__":
    main()
