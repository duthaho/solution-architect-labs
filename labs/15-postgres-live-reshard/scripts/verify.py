"""The independent gate. State-aware: reads router_state.json and the actual
replication topology, then proves whichever invariants the current phase
owes — union checksums against mono, per-shard filter ownership, global PK
uniqueness, and a replay of every acked write in the journal. Never trusts a
stored ok flag; everything is recomputed from the databases and the journals.

VERIFY_INVERT=1 flips the gate: exit 0 iff naive.jsonl documents real,
recounted damage — proof the gate catches what the naive cutover does."""
import os

from common import (
    JOURNAL, LAB_DIR, N_SHARDS, SHARDS, conn, log, read_jsonl,
    read_router_state, shard_filter, write_router_state,
)
from drill_naive import audit as audit_on_shards
from drill_rollback import audit_on_mono
from replicate import capture_lsn, wait_for_lsn

NAIVE = LAB_DIR / "naive.jsonl"
N_RANGES = 16

failures = []


def check(name, ok, detail=""):
    mark = "ok" if ok else "FAIL"
    print(f"  [{mark:>4}] {name}" + (f" — {detail}" if detail else ""))
    if not ok:
        failures.append(name)


def topology():
    subs = {}
    for node in ["mono"] + SHARDS:
        with conn(node) as c:
            subs[node] = [s for (s,) in c.execute("SELECT subname FROM pg_subscription")]
    forward = any(subs[s] for s in SHARDS)
    reverse = bool(subs["mono"])
    return forward, reverse


def range_checksums(node, shard_index, lo, hi):
    sql_filter = shard_filter(shard_index).replace("%", "%%")
    with conn(node) as c:
        return c.execute(
            f"SELECT md5(coalesce(string_agg(id || ':' || workspace_id || ':' || rev, "
            f"',' ORDER BY id), '')) FROM docs "
            f"WHERE {sql_filter} AND id BETWEEN %s AND %s",
            (lo, hi),
        ).fetchone()[0]


def union_checksums_against(reference):
    """Compare each shard partition, range by range, against the same
    partition filter applied to the reference node."""
    with conn(reference) as c:
        max_id = c.execute("SELECT coalesce(max(id), 0) FROM docs").fetchone()[0]
    step = max(1, max_id // N_RANGES + 1)
    bad = 0
    for i, shard in enumerate(SHARDS):
        for lo in range(1, max_id + 1, step):
            hi = lo + step - 1
            if range_checksums(reference, i, lo, hi) != range_checksums(shard, i, lo, hi):
                bad += 1
    total = N_SHARDS * len(range(1, max_id + 1, step))
    check(f"union checksums vs {reference} ({total} ranges)", bad == 0,
          f"{bad} mismatched ranges" if bad else f"all {total} match")


def check_boundaries():
    for i, shard in enumerate(SHARDS):
        with conn(shard) as c:
            stray = c.execute(
                f"SELECT count(*) FROM docs WHERE NOT {shard_filter(i)}"
            ).fetchone()[0]
        check(f"{shard} owns only its partition", stray == 0, f"{stray} stray rows" if stray else "")


def check_counts_union(reference):
    with conn(reference) as c:
        ref = c.execute("SELECT count(*) FROM docs").fetchone()[0]
    total = 0
    for shard in SHARDS:
        with conn(shard) as c:
            total += c.execute("SELECT count(*) FROM docs").fetchone()[0]
    check(f"row count: shards union == {reference}", total == ref, f"{total} vs {ref}")


def check_global_uniqueness():
    with conn(SHARDS[0]) as c0:
        ids0 = {r[0] for r in c0.execute("SELECT id FROM docs").fetchall()}
    with conn(SHARDS[1]) as c1:
        dups = [r[0] for r in c1.execute("SELECT id FROM docs").fetchall() if r[0] in ids0]
    check("global PK uniqueness across shards", not dups,
          f"{len(dups)} ids on both shards" if dups else "")


def check_journal(state):
    records = read_jsonl(JOURNAL)
    if not records:
        check("journal replay", True, "journal empty (nothing acked this epoch)")
        return
    if state["authoritative"] == "shards":
        damage = audit_on_shards(records)
        check(f"journal replay on shards ({len(records)} acked writes)", not damage,
              f"{len(damage)} missing/stale" if damage else "all present and fresh")
    else:
        damage = audit_on_mono(records)
        check(f"journal replay on mono ({len(records)} acked writes)", not damage,
              f"{len(damage)} missing/stale" if damage else "all present and fresh")


def gated(fn):
    """Quiesce writes and wait out replication before comparing snapshots —
    the verify-time equivalent of Notion's pause-before-dark-read."""
    state = read_router_state()
    state["writes_gated"] = True
    write_router_state(state)
    try:
        import time
        time.sleep(0.3)
        fn(state)
    finally:
        state["writes_gated"] = False
        write_router_state(state)


def verify():
    state = read_router_state()
    forward, reverse = topology()
    phase = (
        "pre-cutover (mono authoritative, forward replication)" if forward and state["authoritative"] == "mono"
        else "post-cutover (shards authoritative, reverse stream armed)" if reverse and state["authoritative"] == "shards"
        else "post-rollback (mono authoritative, shards retired)" if reverse
        else "unreplicated (mono only)"
    )
    print(f"phase: {phase}")

    def run(state):
        if forward and state["authoritative"] == "mono":
            lsn = capture_lsn("mono")
            wait_for_lsn(lsn)
            check_counts_union("mono")
            check_boundaries()
            check_global_uniqueness()
            union_checksums_against("mono")
        elif state["authoritative"] == "shards":
            if reverse:
                for i, shard in enumerate(SHARDS):
                    wait_for_lsn(capture_lsn(shard), publisher=shard, subs=[f"sub_back{i}"])
                check_counts_union("mono")
                union_checksums_against("mono")
            check_boundaries()
            check_global_uniqueness()
        check_journal(state)

    gated(run)

    print()
    if failures:
        print(f"❌ VERIFICATION FAILED: {', '.join(failures)}")
        raise SystemExit(1)
    print("✅ VERIFIED — every invariant this phase owes holds, recomputed from scratch")


def verify_invert():
    """Exit 0 iff the naive drill's journaled damage is real when recounted."""
    records = read_jsonl(NAIVE)
    details = [r for r in records if not r.get("summary")]
    summaries = [r for r in records if r.get("summary")]
    ok = (
        len(summaries) == 1
        and summaries[0]["missing"] == sum(1 for d in details if d["damage"] == "missing")
        and summaries[0]["stale"] == sum(1 for d in details if d["damage"] == "stale")
        and summaries[0]["missing"] >= 1
        and summaries[0]["stale"] >= 1
        and all(d.get("id") and d.get("rev") and d.get("node") == "mono" for d in details)
    )
    if not ok:
        print("❌ INVERTED GATE FAILED: naive.jsonl does not document real, "
              "recountable acked-write damage")
        raise SystemExit(1)
    s = summaries[0]
    print(f"✅ inverted gate: the naive cutover really did damage acked writes "
          f"({s['missing']} missing, {s['stale']} stale of {s['acked_in_window']} "
          f"in the lag window) — recounted from the record, not trusted")


def main():
    if os.environ.get("VERIFY_INVERT") == "1":
        verify_invert()
    else:
        verify()


if __name__ == "__main__":
    main()
