"""Seed accounts and reset the world to a known-good state.

Reseeding is the lab's reset button: truncate all mutable state, delete all
race journals, and write seed.json — the conservation baseline that every
drill and verify.py measure against. Strategy d's ledger gets an opening
entry (seq=1) per account so SUM(entries) == balance_cache holds from t0.
"""
import json

import common


def main() -> None:
    conn = common.connect()
    with conn.cursor() as cur:
        cur.execute("TRUNCATE accounts")
        cur.execute("TRUNCATE transfers")
        cur.execute("TRUNCATE entries")
        cur.execute("TRUNCATE balance_cache")
        for i in range(1, common.ACCOUNTS + 1):
            cur.execute(
                "INSERT INTO accounts (id, balance, version) VALUES (%s, %s, 0)",
                (i, common.BALANCE),
            )
            cur.execute(
                "INSERT INTO entries (account_id, seq, amount, transfer_id) "
                "VALUES (%s, 1, %s, NULL)",
                (i, common.BALANCE),
            )
            cur.execute(
                "INSERT INTO balance_cache (account_id, balance, last_seq) "
                "VALUES (%s, %s, 1)",
                (i, common.BALANCE),
            )
    conn.commit()

    total = common.sum_balances(conn)
    conn.close()

    for mode in common.MODES:
        common.journal_path(mode).unlink(missing_ok=True)

    common.SEED_PATH.write_text(
        json.dumps({"accounts": common.ACCOUNTS, "total": str(total)}) + "\n"
    )
    common.log.info(
        "seeded %d accounts x %s = total %s (seed.json written, journals cleared)",
        common.ACCOUNTS, common.BALANCE, total,
    )


if __name__ == "__main__":
    main()
