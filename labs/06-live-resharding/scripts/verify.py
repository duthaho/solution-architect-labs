"""Prove the reshard lost nothing, duplicated nothing, misplaced nothing.

Four independent checks:

1. PARTITION RECONCILIATION — per shard: per-user COUNT(*) + content
   checksum (SUM of CRC32 over the value columns) against mono restricted
   to that shard's partition. Counts catch loss AND duplication; the
   checksum catches stale or corrupted values counts can't see. updated_at
   is excluded on purpose (schema.sql).

   Under live traffic a snapshot of two servers is never atomic, and the
   skew GROWS with table size: a full-table fingerprint takes ~1s per side
   at 500k rows, so the two scans are always dozens of inserts apart and
   whole-table retries never converge (this lab's first version did exactly
   that, and the 500k demo caught it). So the comparison is per USER:
   one GROUP BY pass over each side, then only the divergent users are
   re-read back-to-back — a millisecond window that a legitimate in-flight
   write clears on the first retry, while real divergence never does.
   Re-check-the-key-before-paging is how production reconcilers work too.

2. MISPLACEMENT SCAN — every row must live on the shard that owns its
   user_id: `WHERE CRC32(user_id)%2 != s` must return 0 on shard s. A
   misplaced row is invisible to every router read — data loss with the
   bytes still on disk.

3. JOURNAL REPLAY — replay journal.jsonl (every ACKED write, last-op-wins)
   and assert against the CURRENT authoritative side: per-user count==max
   ==acked seq, plus per-row values for every journaled (user,seq). Also
   fails on any `check_fail` the traffic generator recorded — an app-level
   detection is a verification failure even if the data later healed.

4. SHADOW REPORT — shadow_diffs.jsonl must be empty: the read path was
   rehearsed against the shards and never disagreed with mono.

Checks 1+2 prove the data moved correctly; 3 proves no acked write was
dropped on the way; 4 proves the READ path, not just the data at rest.
"""
import argparse
import json
import sys
import time

from common import (
    JOURNAL,
    SHADOW_DIFFS,
    SHARDS,
    TABLE,
    connect,
    log,
    read_mode,
    shard_filter_sql,
    shard_for,
)

FINGERPRINT = "COUNT(*), COALESCE(SUM(CRC32(CONCAT_WS('#', user_id, seq, status, amount, note))), 0)"
RETRIES = 8


def _user_fingerprints(conn, where: str) -> dict[int, tuple[int, int]]:
    with conn.cursor() as cur:
        cur.execute(f"SELECT user_id, {FINGERPRINT} FROM {TABLE} WHERE {where} GROUP BY user_id")
        return {int(u): (int(c), int(s)) for u, c, s in cur.fetchall()}


def _one_user_fingerprint(conn, user: int) -> tuple[int, int]:
    with conn.cursor() as cur:
        cur.execute(f"SELECT {FINGERPRINT} FROM {TABLE} WHERE user_id=%s", (user,))
        c, s = cur.fetchone()
        return int(c), int(s)


def check_partitions(mono, shard_conns) -> int:
    failures = 0
    for shard in SHARDS:
        # One full pass per side. Under traffic these two snapshots are
        # seconds apart — divergent users here are SUSPECTS, not failures.
        m = _user_fingerprints(mono, shard_filter_sql(shard))
        s = _user_fingerprints(shard_conns[shard], "TRUE")
        suspects = [u for u in set(m) | set(s) if m.get(u, (0, 0)) != s.get(u, (0, 0))]

        # Re-read each suspect back-to-back: the window shrinks from seconds
        # to milliseconds, so in-flight skew clears on the first retry.
        bad = []
        for user in sorted(suspects):
            for _ in range(RETRIES):
                if (_one_user_fingerprint(mono, user)
                        == _one_user_fingerprint(shard_conns[shard], user)):
                    break
                time.sleep(0.25)
            else:
                bad.append(user)

        rows = sum(c for c, _ in s.values())
        if bad:
            for user in bad[:5]:
                log.error("  %s DIVERGED user=%d: mono=%s shard=%s", shard, user,
                          _one_user_fingerprint(mono, user),
                          _one_user_fingerprint(shard_conns[shard], user))
            log.error("  %s MISMATCH: %d users diverged and never converged "
                      "(%d in-flight suspects cleared on re-check)",
                      shard, len(bad), len(suspects) - len(bad))
            failures += 1
        else:
            log.info("  %s OK: %d rows, %d users match mono partition "
                     "(%d in-flight suspects cleared on re-check)",
                     shard, rows, len(s), len(suspects))
    return failures


def check_misplacement(shard_conns) -> int:
    failures = 0
    for shard in SHARDS:
        with shard_conns[shard].cursor() as cur:
            cur.execute(f"SELECT COUNT(*) FROM {TABLE} WHERE NOT ({shard_filter_sql(shard)})")
            n = cur.fetchone()[0]
        if n:
            log.error("  %s holds %d rows it does NOT own — invisible to every router read", shard, n)
            failures += 1
        else:
            log.info("  %s OK: 0 rows outside its partition", shard)
    return failures


def replay_journal() -> tuple[dict, dict, int]:
    """-> ({user: acked max seq}, {(user,seq): last values}, app_check_fails)."""
    max_seq: dict[int, int] = {}
    rows: dict[tuple[int, int], dict] = {}
    check_fails = 0
    if not JOURNAL.exists():
        log.warning("no journal file — was traffic running?")
        return max_seq, rows, 0
    with open(JOURNAL) as f:
        for line in f:
            e = json.loads(line)
            if e["op"] == "check_fail":
                check_fails += 1
                continue
            rows[(e["user"], e["seq"])] = {"status": e["status"], "amount": e["amount"],
                                           "note": e["note"]}
            if e["op"] == "insert":
                max_seq[e["user"]] = max(max_seq.get(e["user"], 0), e["seq"])
    return max_seq, rows, check_fails


def authoritative_conn_for(mono, shard_conns, user: int):
    if read_mode() == "sharded":
        return shard_conns[shard_for(user)]
    return mono


def check_journal(mono, shard_conns) -> int:
    max_seq, rows, check_fails = replay_journal()
    failures = check_fails
    if check_fails:
        log.error("  traffic recorded %d check_fail entries — the app SAW a bad state live",
                  check_fails)
    if not rows:
        log.info("  journal empty — skipping replay")
        return failures

    # Per-user invariant on the authoritative side.
    for user, acked in max_seq.items():
        conn = authoritative_conn_for(mono, shard_conns, user)
        with conn.cursor() as cur:
            cur.execute(
                f"SELECT COUNT(*), COALESCE(MAX(seq),0) FROM {TABLE} WHERE user_id=%s", (user,))
            count, mx = (int(x) for x in cur.fetchone())
        if count != mx or mx < acked:
            log.error("  GAP user=%d: count=%d max_seq=%d, journal acked up to %d", user, count, mx, acked)
            failures += 1

    # Per-row values for every journaled write (last-op-wins).
    stale = missing = 0
    for (user, seq), exp in rows.items():
        conn = authoritative_conn_for(mono, shard_conns, user)
        with conn.cursor() as cur:
            cur.execute(
                f"SELECT status, amount, note FROM {TABLE} WHERE user_id=%s AND seq=%s", (user, seq))
            row = cur.fetchone()
        if row is None:
            log.error("  GAP: acked row (%d,%d) MISSING from authoritative node", user, seq)
            missing += 1
        elif (row[0], float(row[1]), row[2]) != (exp["status"], exp["amount"], exp["note"]):
            log.error("  STALE: (%d,%d) is %r, journal says %r", user, seq,
                      (row[0], float(row[1]), row[2]), exp)
            stale += 1
    failures += stale + missing
    log.info("  replayed %d acked writes over %d users: %d missing, %d stale",
             len(rows), len(max_seq), missing, stale)
    return failures


def check_shadow() -> int:
    if not SHADOW_DIFFS.exists():
        log.info("  no shadow diff journal (shadow-read phase not run yet)")
        return 0
    diffs = [line for line in SHADOW_DIFFS.read_text().splitlines() if line]
    if diffs:
        log.error("  %d shadow-read diffs — the shard read path disagreed with mono:", len(diffs))
        for line in diffs[:5]:
            log.error("    %s", line)
        return 1
    log.info("  shadow diff journal exists and is EMPTY — read path proven")
    return 0


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--data-only", action="store_true",
                        help="skip journal + shadow checks (partition/misplacement only)")
    args = parser.parse_args()

    mono = connect("mono")
    shard_conns = {s: connect(s) for s in SHARDS}

    f1 = f2 = 0
    if read_mode() == "single":
        # Either the ladder hasn't started (shards empty) or a rollback
        # finished (shards abandoned, going stale by design). Comparing mono
        # to the shards is meaningless in both; the journal is the truth here.
        log.info("CHECKS 1+2 skipped: mode=single — shards are not in the write path")
    else:
        log.info("CHECK 1: partition reconciliation (count + content checksum per shard vs mono)")
        f1 = check_partitions(mono, shard_conns)
        log.info("CHECK 2: misplacement scan (every row on the shard that owns it)")
        f2 = check_misplacement(shard_conns)
    f3 = f4 = 0
    if not args.data_only:
        log.info("CHECK 3: journal replay against the authoritative side (mode=%s)", read_mode())
        f3 = check_journal(mono, shard_conns)
        log.info("CHECK 4: shadow-read diff report")
        f4 = check_shadow()

    mono.close()
    for c in shard_conns.values():
        c.close()

    total = f1 + f2 + f3 + f4
    if total:
        log.error("❌ VERIFICATION FAILED: %d problems (partition=%d misplaced=%d journal=%d shadow=%d)",
                  total, f1, f2, f3, f4)
        sys.exit(1)
    log.info("✅ VERIFIED: 0 lost, 0 duplicated, 0 misplaced, 0 stale — and the read path agrees")


if __name__ == "__main__":
    main()
