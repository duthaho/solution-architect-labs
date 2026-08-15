"""The money table: every number measured on THIS machine in THIS run,
assembled from audit_summary.json (soak staleness) and herd_summary.json
(stampede DB cost). Only the cost/complexity column is an opinion.
"""
import json

from common import LAB_DIR

AUDIT = LAB_DIR / "audit_summary.json"
HERD = LAB_DIR / "herd_summary.json"

VERDICT = {
    "ttl":       ("none (SETEX)",            "staleness = TTL, always; cheap and honest"),
    "delete":    ("1 extra DEL per write",   "usually fresh; loses to the stale-set race"),
    "versioned": ("+1 RTT per read, orphans", "race-proof; Redis holds the truth pointer"),
    "cdc":       ("Kafka+Debezium pipeline", "bounded by pipeline lag; survives app bugs"),
}


def main() -> None:
    audit = json.loads(AUDIT.read_text()) if AUDIT.exists() else {}
    herd = json.loads(HERD.read_text()) if HERD.exists() else {}

    print("\n" + "=" * 100)
    print("THE MONEY TABLE — measured staleness (soak) and herd cost, this machine, this run")
    print("=" * 100)
    print(f"{'strategy':<11} {'stale reads':>12} {'staleness p99':>14} {'staleness MAX':>14}"
          f"  {'invalidation cost':<24} verdict")
    print("-" * 100)
    for s in ("ttl", "delete", "versioned", "cdc"):
        cost, verdict = VERDICT[s]
        a = audit.get(s)
        if a:
            print(f"{s:<11} {a['stale_pct']:>11}% {a['p99_ms']:>12}ms {a['max_ms']:>12}ms"
                  f"  {cost:<24} {verdict}")
        else:
            note = "run `make drill-cdc`" if s == "cdc" else "run `make soak`"
            print(f"{s:<11} {'—':>12} {'—':>14} {'—':>14}  {cost:<24} ({note})")
    if herd:
        n = herd["readers"]
        print("-" * 100)
        print(f"herd ({n} readers, 1 expired hot key):  "
              f"naive={herd['naive']['db_reads']} DB reads  "
              f"singleflight={herd['singleflight']['db_reads']}  "
              f"swr={herd['swr']['db_reads']} (p99 "
              f"{herd['naive']['p99_ms']}/{herd['singleflight']['p99_ms']}"
              f"/{herd['swr']['p99_ms']}ms)")
    print("=" * 100)
    print("staleness numbers come from journal joins (auditor.py), not from the strategies' claims.")


if __name__ == "__main__":
    main()
