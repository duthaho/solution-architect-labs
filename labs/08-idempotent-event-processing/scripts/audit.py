"""The ledger audit — this lab's ground truth.

Expected balance per account = seed + SUM(payments.amount_cents): every
unique payment the business accepted, applied exactly once. Actual balance =
whatever the consumer left in `accounts`. Any difference is corruption —
overcharge (events applied more than once) or loss (events never applied).

The uncomfortable production lesson this file embodies: balance corruption
raises NO error anywhere. The consumer is green, lag is zero, dashboards are
happy. Only an independent reconciliation against the source of truth finds
it — which is why payment companies run one continuously (README §8).

--expect clean|corrupted turns the audit into a drill gate: exit non-zero if
reality doesn't match what the drill was supposed to demonstrate.
"""
import argparse
import json
import sys

from common import LAB_DIR, SEED_BALANCE_CENTS, connect_mysql, log


def fmt(cents: int) -> str:
    return f"{cents / 100:+,.2f}"


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--expect", choices=["clean", "corrupted"])
    args = ap.parse_args()

    conn = connect_mysql()
    with conn.cursor() as cur:
        cur.execute(
            """SELECT a.id,
                      %s + COALESCE(SUM(p.amount_cents), 0) AS expected,
                      a.balance_cents                        AS actual
               FROM accounts a LEFT JOIN payments p ON p.account_id = a.id
               GROUP BY a.id, a.balance_cents
               ORDER BY a.id""",
            (SEED_BALANCE_CENTS,))
        rows = cur.fetchall()
        cur.execute("SELECT COUNT(*) FROM payments")
        n_payments = cur.fetchone()[0]
        cur.execute("SELECT COUNT(*) FROM processed_events")
        n_processed = cur.fetchone()[0]
    conn.close()

    diffs = [(acc, int(expected), int(actual), int(actual) - int(expected))
             for acc, expected, actual in rows if int(actual) != int(expected)]
    overcharge = sum(d for *_, d in diffs if d > 0)
    loss = sum(-d for *_, d in diffs if d < 0)

    print()
    print("=" * 64)
    print("LEDGER AUDIT  (expected = seed + each unique payment once)")
    print("=" * 64)
    print(f"  unique payments accepted : {n_payments}")
    print(f"  dedupe ledger entries    : {n_processed}"
          + ("   (naive mode keeps none)" if n_processed == 0 else ""))
    print(f"  accounts checked         : {len(rows)}")
    print(f"  accounts corrupted       : {len(diffs)}")
    print(f"  money over-applied       : ${fmt(overcharge)}   (duplicate effects)")
    print(f"  money lost               : ${fmt(-loss)}   (events never applied)")
    if diffs:
        print("  worst offenders (account, expected, actual, drift):")
        for acc, expected, actual, d in sorted(diffs, key=lambda r: -abs(r[3]))[:5]:
            print(f"    #{acc:<4d} ${expected / 100:>12,.2f}  ${actual / 100:>12,.2f}  {fmt(d)}")
    verdict = "CORRUPTED" if diffs else "CLEAN"
    print(f"  VERDICT: {verdict}")
    print("=" * 64)

    (LAB_DIR / "audit.json").write_text(json.dumps({
        "verdict": verdict,
        "payments": n_payments,
        "accounts_corrupted": len(diffs),
        "overcharge_cents": overcharge,
        "lost_cents": loss,
    }))

    if args.expect and args.expect != verdict.lower():
        log.error("Expected %s but ledger is %s — the drill did not demonstrate "
                  "what it claims. Investigate before trusting anything above.",
                  args.expect.upper(), verdict)
        sys.exit(1)


if __name__ == "__main__":
    main()
