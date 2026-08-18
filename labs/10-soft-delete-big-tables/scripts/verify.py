"""Prove the exactly-once invariant for every family that has a journal.

Sources of truth joined together:
  traffic_<f>.jsonl   what the app was told happened (acked ops only)
  outcome_<f>.jsonl   what the delete strategy claims it did
  the database        what is actually there

Rules per journaled (table, id), using the LAST outcome for that row:
  family a:  soft_deleted -> row in live, deleted_at set
             no outcome   -> row in live (drills may have flagged it)
  family b:  moved        -> row in mirror and NOT in live
             no outcome   -> row in live XOR mirror (a cascade or drill may
                             legitimately have moved it with its parent)
  family c:  purged       -> row nowhere
             soft_deleted -> row in flagged-live XOR archive (archiver timing)
             no outcome   -> row in live XOR archive (drills archive rows too)

Plus, for b and c: a global overlap scan — no id may exist on both sides,
journaled or not. Exit 2 with violation samples if anything fails.
"""
import sys

from common import (FAMILIES, connect, journal_path, log, outcome_path,
                    read_jsonl)

CHECK_TABLES = ("users", "orders")  # the journal only references these


def exists(cur, schema: str, table: str, id_: int, where: str = "1=1") -> bool:
    cur.execute(f"SELECT 1 FROM `{schema}`.`{table}` WHERE id=%s AND {where}", (id_,))
    return cur.fetchone() is not None


def overlap_count(cur, s1: str, s2: str, table: str) -> int:
    cur.execute(f"SELECT COUNT(*) FROM `{s1}`.`{table}` a JOIN `{s2}`.`{table}` b "
                f"ON a.id=b.id")
    return cur.fetchone()[0]


def verify_family(cur, family: str) -> list[str]:
    journal = read_jsonl(journal_path(family))
    if not journal:
        log.info("[%s] no journal — skipped", family)
        return []
    cfg = FAMILIES[family]
    live = cfg["live"]
    other = cfg.get("deleted") or cfg.get("archive")

    last_action: dict[tuple[str, int], str] = {}
    for o in read_jsonl(outcome_path(family)):
        last_action[(o["table"], o["id"])] = o["action"]

    ids = {(r["table"], r["id"]) for r in journal if r["table"] in CHECK_TABLES}
    bad: list[str] = []
    for table, id_ in sorted(ids):
        action = last_action.get((table, id_))
        in_live = exists(cur, live, table, id_)
        in_other = exists(cur, other, table, id_) if other else False
        ok = True
        if family == "a":
            ok = in_live and (action != "soft_deleted"
                              or exists(cur, live, table, id_, "deleted_at IS NOT NULL"))
        elif family == "b":
            ok = (in_other and not in_live) if action == "moved" \
                else (in_live != in_other)  # XOR: cascades move children too
        elif family == "c":
            if action == "purged":
                ok = not in_live and not in_other
            elif action == "soft_deleted":
                ok = (exists(cur, live, table, id_, "deleted_at IS NOT NULL")
                      != in_other)  # XOR: flagged-live or archived, never both/neither
            else:
                ok = in_live != in_other  # XOR
        if not ok:
            bad.append(f"[{family}] {table} id={id_} action={action} "
                       f"live={in_live} other={in_other}")

    if other:
        for table in ("users", "orders", "order_items"):
            n = overlap_count(cur, live, other, table)
            if n:
                bad.append(f"[{family}] {table}: {n} ids exist in BOTH {live} and {other}")

    log.info("[%s] checked %d journaled rows -> %d violations", family, len(ids), len(bad))
    return bad


def main() -> None:
    conn = connect()
    bad: list[str] = []
    with conn.cursor() as cur:
        for family in FAMILIES:
            bad += verify_family(cur, family)
    conn.close()
    if bad:
        log.error("verify FAILED — %d violations, first 10:", len(bad))
        for line in bad[:10]:
            log.error("  %s", line)
        sys.exit(2)
    log.info("verify: OK — every acked op is accounted for, every id lives in "
             "exactly one place")


if __name__ == "__main__":
    main()
