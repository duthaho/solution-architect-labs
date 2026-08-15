"""The loss report: diff what the database PROMISED (journal) against what
SURVIVED (the new primary), and print the failover timeline.

RPO here is measured, not estimated: every acked seq missing from the current
primary is a row the application was told was durable — and which is now gone.
"""
import argparse
import json

from common import (JOURNAL, NODES, RESULTS, connect, current_primary,
                    gtid_executed, is_alive, log, query_one, timeline_get)


def read_journal() -> tuple[set[int], set[int], dict]:
    acked, ambiguous, stats = set(), set(), {}
    if JOURNAL.exists():
        for line in JOURNAL.read_text().splitlines():
            rec = json.loads(line)
            if rec["type"] == "acked":
                acked.add(rec["seq"])
            elif rec["type"] == "ambiguous":
                ambiguous.add(rec["seq"])
            elif rec["type"] == "stats":
                stats = rec
    return acked, ambiguous - acked, stats   # a later ack resolves an earlier ambiguity


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--label", default=None,
                    help="save the result under this label in results.jsonl (for the demo comparison table)")
    args = ap.parse_args()

    acked, ambiguous, stats = read_journal()
    primary = current_primary()
    conn = connect(primary)
    with conn.cursor() as cur:
        cur.execute("SELECT seq FROM events WHERE payload LIKE 'traffic-%%'")
        present = {r["seq"] for r in cur.fetchall()}

    lost = sorted(acked - present)
    unacked_present = sorted(present - acked - ambiguous)
    ambiguous_landed = sorted(a for a in ambiguous if a in present)
    ambiguous_gone = sorted(a for a in ambiguous if a not in present)

    print()
    print("=" * 64)
    print(f"  LOSS REPORT (current primary: {primary})")
    print("=" * 64)
    print(f"  acked by primary            : {len(acked):6d} rows")
    print(f"  acked and present           : {len(acked & present):6d} rows")
    print(f"  acked but LOST after failover: {len(lost):5d} rows   <-- RPO")
    if lost:
        print(f"      lost seq range: {lost[0]} .. {lost[-1]}")
    print(f"  in-flight, no ack (ambiguous): {len(ambiguous) :5d} rows"
          f"  (landed: {len(ambiguous_landed)}, gone: {len(ambiguous_gone)})")
    if unacked_present:
        print(f"  present but never acked      : {len(unacked_present):5d} rows (committed as the crash hit)")
    if stats:
        print(f"  writer commit latency        : avg {stats['avg_commit_ms']:.2f}ms, "
              f"p95 {stats['p95_commit_ms']:.2f}ms")

    # -- timeline / RTO --
    tl = timeline_get()
    if "kill_ts" in tl:
        print("-" * 64)
        print("  TIMELINE")
        base = tl["kill_ts"]
        for name in ("lag_injected_ts", "kill_ts", "detect_ts", "candidate_ts",
                     "relay_drained_ts", "promoted_ts", "router_flip_ts"):
            if name in tl:
                print(f"    {name:18s} T{(tl[name]-base)/1000.0:+8.3f}s")
        if "router_flip_ts" in tl:
            rto_s = (tl["router_flip_ts"] - tl["kill_ts"]) / 1000.0
            print(f"    RTO (kill -> router flip)   {rto_s:8.3f}s")

    # -- GTID snapshot of every surviving node: the post-mortem raw material --
    print("-" * 64)
    print("  GTID_EXECUTED SNAPSHOT")
    for node in NODES:
        if is_alive(node):
            c = connect(node, db=None)
            print(f"    {node:9s} {gtid_executed(c) or '(empty)'}")
            c.close()
        else:
            print(f"    {node:9s} DEAD")
    print("=" * 64)

    if args.label:
        tl = timeline_get()
        rto_s = ((tl.get("router_flip_ts", 0) - tl.get("kill_ts", 0)) / 1000.0) if "kill_ts" in tl else None
        with open(RESULTS, "a") as f:
            f.write(json.dumps({
                "label": args.label, "acked": len(acked), "lost": len(lost),
                "ambiguous": len(ambiguous), "rto_s": rto_s,
                "avg_commit_ms": stats.get("avg_commit_ms"),
                "p95_commit_ms": stats.get("p95_commit_ms"),
            }) + "\n")
        log.info("result saved to results.jsonl under label %r", args.label)


if __name__ == "__main__":
    main()
