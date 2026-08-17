"""The grader. Three independent checks, three subcommands (default: report):

report   Read the client's anomaly journal: counts by kind + the ERROR WINDOW
         (first anomaly -> last anomaly, wall-clock ms). This is the number
         the whole lab is about: naive prints thousands of failures over tens
         of seconds; the ladder prints "error window: none".
         --expect-clean / --expect-carnage turn it into a gate.

shapes   At-rest schema-shape agreement: (a) rows still missing first/last
         (must be 0 after backfill); (b) rows where `name` disagrees with the
         split of itself (must be 0 while dual-writing — proves app split,
         library split and backfill SQL all agree). (b) is only meaningful
         while both shapes are being written; after v3 `name` goes stale by
         design — use --nulls-only then (contract.py does).

audit    The acked-write audit, lab 04's RPO idea: replay client_state.json
         (every write the app ever ACKED) against the database NOW. Any row
         whose current value differs from its last acked write is a lost or
         mangled write — the naive deploy produces them, the ladder must not.
"""
import argparse
import json
import sys

import common as c


def report(expect: str | None) -> int:
    if not c.CLIENT_JOURNAL.exists():
        print("no client journal — start traffic first")
        return 2
    entries = [json.loads(l) for l in c.CLIENT_JOURNAL.read_text().splitlines() if l]
    by_kind: dict[str, int] = {}
    for e in entries:
        by_kind[e["kind"]] = by_kind.get(e["kind"], 0) + 1
    total = sum(by_kind.values())
    print(f"journal report: {total} anomalies "
          f"({', '.join(f'{k}={v}' for k, v in sorted(by_kind.items())) or 'none'})")
    if entries:
        window_ms = entries[-1]["ts_ms"] - entries[0]["ts_ms"]
        print(f"  ERROR WINDOW: {window_ms}ms "
              f"(first anomaly -> last anomaly, wall clock)")
        for e in entries[:3]:
            print(f"  e.g. {e}")
    else:
        print("  ERROR WINDOW: none — zero anomalies")
    if expect == "clean" and total:
        print("❌ expected a clean run, got anomalies")
        return 2
    if expect == "carnage" and not total:
        print("❌ expected the drill to produce anomalies but the journal is clean "
              "— the drill no longer demonstrates anything")
        return 2
    return 0


def shapes(nulls_only: bool) -> int:
    cols = c.columns()
    if not {"first_name", "last_name"} <= cols:
        print("shapes: first/last columns absent — run expand first")
        return 2
    failed = False
    with c.connect().cursor() as cur:
        cur.execute(f"SELECT COUNT(*) FROM {c.TABLE} WHERE first_name IS NULL")
        nulls = cur.fetchone()[0]
        print(f"shapes: rows missing first/last: {nulls}")
        failed |= nulls > 0
        if "name" in cols and not nulls_only:
            cur.execute(f"SELECT COUNT(*) FROM {c.TABLE} WHERE name IS NOT NULL AND "
                        f"(first_name <> {c.SQL_FIRST} OR last_name <> {c.SQL_LAST})")
            disagree = cur.fetchone()[0]
            print(f"shapes: rows where name disagrees with first/last: {disagree}")
            failed |= disagree > 0
    print("❌ shape check FAILED" if failed else "✅ shapes agree")
    return 2 if failed else 0


def audit() -> int:
    if not c.CLIENT_STATE.exists():
        print("no client_state.json — stop traffic first (the client dumps it on exit)")
        return 2
    acked = json.loads(c.CLIENT_STATE.read_text())
    cols = c.columns()
    lost, examples = 0, []
    with c.connect().cursor() as cur:
        for uid, expected in acked.items():
            if "name" in cols:
                cur.execute(f"SELECT name FROM {c.TABLE} WHERE id=%s", (uid,))
                row = cur.fetchone()
                current = row[0] if row else None
            else:
                cur.execute(f"SELECT first_name, last_name FROM {c.TABLE} WHERE id=%s",
                            (uid,))
                row = cur.fetchone()
                current = c.compose_name(row[0], row[1]) if row else None
            if current != expected:
                lost += 1
                if len(examples) < 3:
                    examples.append((uid, expected, current))
    print(f"acked-write audit: {len(acked)} acked writes, {lost} LOST or mangled")
    for uid, exp, cur_ in examples:
        print(f"  id={uid}: acked {exp!r}, database now has {cur_!r}")
    if lost:
        print("❌ the database disagrees with writes the app acknowledged")
        return 2
    print("✅ every acked write is intact")
    return 0


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("check", nargs="?", default="report",
                    choices=["report", "shapes", "audit"])
    ap.add_argument("--expect-clean", action="store_true")
    ap.add_argument("--expect-carnage", action="store_true")
    ap.add_argument("--nulls-only", action="store_true",
                    help="shapes: skip the name-vs-split agreement check "
                         "(after v3, name is stale by design)")
    args = ap.parse_args()
    if args.check == "report":
        return report("clean" if args.expect_clean
                      else "carnage" if args.expect_carnage else None)
    if args.check == "shapes":
        return shapes(args.nulls_only)
    return audit()


if __name__ == "__main__":
    sys.exit(main())
