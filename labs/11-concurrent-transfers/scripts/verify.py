"""The invariant gate. Joins three sources of truth:

  1. seed.json            — what the world started with,
  2. race_<mode>.jsonl    — what clients believe was acked,
  3. the database         — accounts, transfers, entries, balance_cache.

and asserts, for whatever journals exist:

  * conservation: balances sum to the seeded total (accounts for modes
    naive/a/b/c, balance_cache + entries for mode d),
  * no account is negative anywhere,
  * per-account balances equal seed + the journal-implied acked deltas,
  * exactly-once: every acked journal line has exactly one `transfers` row
    and vice versa (join key: transfer_id); mode-d acks have exactly the
    debit+credit pair in `entries`,
  * the ledger read model reconciles: balance_cache == SUM(entries) per
    account.

Exit 0 clean, exit 1 with every violation printed. Run after the naive race
(`make verify-naive`) this MUST fail — that failing exit code is the lab's
proof that the checker can see the corruption.
"""
import sys
from collections import Counter
from decimal import Decimal

import common

ACCOUNT_MODES = ("naive", "a", "b", "c")  # journals applied to `accounts`


def main() -> None:
    seed = common.read_seed()
    seed_total = Decimal(seed["total"])
    n = seed["accounts"]
    per_account = seed_total / n

    journals = {
        mode: common.read_jsonl(common.journal_path(mode))
        for mode in common.MODES
        if common.journal_path(mode).exists()
    }
    acked = {m: [r for r in recs if r["ok"]] for m, recs in journals.items()}

    conn = common.connect()
    violations: list[str] = []

    def check(ok: bool, msg: str) -> None:
        print(("PASS  " if ok else "FAIL  ") + msg)
        if not ok:
            violations.append(msg)

    # --- conservation + negatives, per balance store -------------------
    accounts = common.read_balances(conn, "a")
    cache = common.read_balances(conn, "d")
    check(sum(accounts.values()) == seed_total,
          f"conservation[accounts]: SUM={sum(accounts.values())} vs seeded {seed_total}")
    check(sum(cache.values()) == seed_total,
          f"conservation[balance_cache]: SUM={sum(cache.values())} vs seeded {seed_total}")
    with conn.cursor() as cur:
        cur.execute("SELECT COALESCE(SUM(amount),0) FROM entries")
        entries_total = Decimal(cur.fetchone()[0])
    check(entries_total == seed_total,
          f"conservation[entries]: SUM={entries_total} vs seeded {seed_total}")
    for mode in ("a", "d"):
        neg = common.negative_accounts(conn, mode)
        check(not neg, f"no negative balances in {common.balance_table(mode)[0]}: {neg or 'none'}")

    # --- per-account: seed + acked journal deltas == actual ------------
    for store, modes, actual in (
        ("accounts", ACCOUNT_MODES, accounts),
        ("balance_cache", ("d",), cache),
    ):
        expected = {aid: per_account for aid in actual}
        for mode in modes:
            for rec in acked.get(mode, []):
                amt = Decimal(rec["amount"])
                expected[rec["src"]] -= amt
                expected[rec["dst"]] += amt
        bad = [aid for aid in actual if expected[aid] != actual[aid]]
        check(not bad,
              f"journal-implied balances match {store} "
              f"(journals: {[m for m in modes if m in acked]}): "
              + (f"{len(bad)} mismatched -> {bad}" if bad else "all match"))

    # --- exactly-once: journal acks <-> transfers rows ------------------
    with conn.cursor() as cur:
        cur.execute("SELECT transfer_id, mode FROM transfers")
        db_rows = cur.fetchall()
    db_by_mode: dict[str, Counter] = {}
    for tid, mode in db_rows:
        db_by_mode.setdefault(mode, Counter())[tid] += 1
    for mode, recs in acked.items():
        j_tids = Counter(r["transfer_id"] for r in recs)
        d_tids = db_by_mode.get(mode, Counter())
        missing = [t for t in j_tids if d_tids[t] != 1]
        extra = [t for t in d_tids if j_tids[t] != 1]
        check(not missing and not extra,
              f"exactly-once[{mode}]: {len(j_tids)} acked lines vs "
              f"{sum(d_tids.values())} transfers rows "
              f"(missing={len(missing)}, unacked/dup={len(extra)})")

    # --- ledger internals ------------------------------------------------
    if "d" in acked:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT transfer_id, COUNT(*) FROM entries "
                "WHERE transfer_id IS NOT NULL GROUP BY transfer_id "
                "HAVING COUNT(*) <> 2"
            )
            bad_pairs = cur.fetchall()
        check(not bad_pairs,
              f"every ledger transfer is a debit+credit pair: "
              f"{len(bad_pairs)} malformed")
    with conn.cursor() as cur:
        cur.execute(
            "SELECT c.account_id FROM balance_cache c "
            "JOIN (SELECT account_id, SUM(amount) s FROM entries "
            "      GROUP BY account_id) e ON e.account_id = c.account_id "
            "WHERE c.balance <> e.s"
        )
        drifted = [r[0] for r in cur.fetchall()]
    check(not drifted, f"balance_cache == SUM(entries) per account: "
          + (f"{len(drifted)} drifted -> {drifted}" if drifted else "all match"))

    conn.close()
    print(f"\nchecked journals: {sorted(journals)} | "
          f"acked total: {sum(len(v) for v in acked.values())}")
    if violations:
        print(f"VERDICT: {len(violations)} violation(s)")
        sys.exit(1)
    print("VERDICT: all invariants hold")


if __name__ == "__main__":
    main()
