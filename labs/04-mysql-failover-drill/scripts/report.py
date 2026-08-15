"""Comparison table across drill results (results.jsonl, written by verify.py)."""
import json

from common import RESULTS

LABELS = {"async": "async replication",
          "semisync": "semi-sync (AFTER_SYNC)",
          "degrade": "semi-sync DEGRADED"}


def main() -> None:
    if not RESULTS.exists():
        raise SystemExit("no results.jsonl — run a drill first")
    rows = {}
    for line in RESULTS.read_text().splitlines():
        rec = json.loads(line)
        rows[rec["label"]] = rec          # last run of each label wins

    print()
    print("=" * 78)
    print("  SAME CRASH, DIFFERENT REPLICATION MODE — THE COMPARISON THAT MATTERS")
    print("=" * 78)
    hdr = f"  {'mode':<24} {'acked':>7} {'LOST (RPO)':>11} {'RTO':>8} {'avg commit':>11} {'p95':>8}"
    print(hdr)
    print("  " + "-" * 74)
    for label, name in LABELS.items():
        if label not in rows:
            continue
        r = rows[label]
        rto = f"{r['rto_s']:.3f}s" if r.get("rto_s") else "-"
        avg = f"{r['avg_commit_ms']:.2f}ms" if r.get("avg_commit_ms") else "-"
        p95 = f"{r['p95_commit_ms']:.2f}ms" if r.get("p95_commit_ms") else "-"
        print(f"  {name:<24} {r['acked']:>7} {r['lost']:>11} {rto:>8} {avg:>11} {p95:>8}")
    print("  " + "-" * 74)
    if "async" in rows and "semisync" in rows:
        print(f"  async lost {rows['async']['lost']} acked rows; semi-sync lost "
              f"{rows['semisync']['lost']} under the identical partition + kill.")
        print("  Durability is a setting, not a hope. The latency column is its price tag.")
    print("=" * 78)


if __name__ == "__main__":
    main()
