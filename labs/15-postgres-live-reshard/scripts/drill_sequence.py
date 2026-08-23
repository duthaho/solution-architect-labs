"""Logical replication copies rows, not sequences: after the cutover every
shard's docs_id_seq still sits at its start value. One shard fails loudly with
a duplicate key; the other mints a SILENT global duplicate — an id that
already exists on its sibling. Then the tempting fix (setval to the local
max) still lets both shards mint the same ids. The fix that holds is
interleaved sequences: increment 2, disjoint parity per shard."""
import psycopg

from common import SHARDS, conn, log, read_router_state, write_router_state


def seq_last_values():
    out = {}
    for shard in SHARDS:
        with conn(shard) as c:
            out[shard] = c.execute("SELECT last_value FROM docs_id_seq").fetchone()[0]
    return out


def try_insert(node, ws):
    with conn(node) as c:
        try:
            row = c.execute(
                "INSERT INTO docs (workspace_id, title, body) "
                "VALUES (%s, 'seq-drill', 'seq-drill') RETURNING id",
                (ws,),
            ).fetchone()
            return row[0], None
        except psycopg.errors.UniqueViolation as e:
            return None, str(e).splitlines()[0]


def exists_globally(doc_id, exclude=None):
    for shard in SHARDS:
        if shard == exclude:
            continue
        with conn(shard) as c:
            if c.execute("SELECT 1 FROM docs WHERE id = %s", (doc_id,)).fetchone():
                return shard
    return None


def cleanup(rows):
    for shard, doc_id in rows:
        with conn(shard) as c:
            c.execute("DELETE FROM docs WHERE id = %s AND title = 'seq-drill'", (doc_id,))


def global_max():
    maxima = []
    for shard in SHARDS:
        with conn(shard) as c:
            maxima.append(c.execute("SELECT coalesce(max(id), 0) FROM docs").fetchone()[0])
    return max(maxima)


def main():
    if read_router_state()["authoritative"] != "shards":
        raise SystemExit("run after the gated cutover: shards must be authoritative")
    trap_rows = []

    print("\n--- 1. the trap: shard sequences never moved ---")
    for shard, last in seq_last_values().items():
        print(f"{shard}: docs_id_seq last_value = {last} (rows on shard go far beyond it)")
    failures = 0
    for i, shard in enumerate(SHARDS):
        ws = i  # ws 0 -> shard0, ws 1 -> shard1: correctly-routed inserts
        doc_id, err = try_insert(shard, ws)
        if err:
            print(f"{shard}: INSERT → ❌ {err}")
            failures += 1
        else:
            dup_on = exists_globally(doc_id, exclude=shard)
            trap_rows.append((shard, doc_id))
            if dup_on:
                print(f"{shard}: INSERT → id {doc_id} 'succeeded' — but id {doc_id} "
                      f"already exists on {dup_on}: a SILENT global duplicate")
                failures += 1
            else:
                print(f"{shard}: INSERT → id {doc_id} (no collision this time)")
    if failures == 0:
        print("❌ drill failed to reproduce any sequence failure")
        raise SystemExit(1)

    print("\n--- 2. the tempting fix that still breaks: setval(local max) ---")
    for shard in SHARDS:
        with conn(shard) as c:
            c.execute("SELECT setval('docs_id_seq', (SELECT max(id) FROM docs))")
    ids = {}
    for i, shard in enumerate(SHARDS):
        doc_id, err = try_insert(shard, i)
        ids[shard] = doc_id
        if doc_id is not None:
            trap_rows.append((shard, doc_id))
    print(f"next ids minted: {ids}")
    if len(set(ids.values())) < len(ids):
        print("❌ both shards minted the SAME id — global uniqueness is gone")
    else:
        print(f"❌ ids differ this time only because local maxima differ — both "
              f"sequences now race up the same range")

    cleanup(trap_rows)

    print("\n--- 3. the fix that holds: interleaved sequences ---")
    base = global_max() + 1
    for i, shard in enumerate(SHARDS):
        start = base + ((i - base) % 2)  # shard0 even parity offset, shard1 odd
        with conn(shard) as c:
            c.execute(f"ALTER SEQUENCE docs_id_seq RESTART WITH {start} INCREMENT BY 2")
        print(f"{shard}: RESTART WITH {start} INCREMENT BY 2 "
              f"({'even' if start % 2 == 0 else 'odd'} ids only)")

    minted = {s: [] for s in SHARDS}
    for _ in range(10):
        for i, shard in enumerate(SHARDS):
            doc_id, err = try_insert(shard, i)
            if err:
                print(f"❌ {shard}: unexpected collision after fix: {err}")
                raise SystemExit(1)
            minted[shard].append(doc_id)
    overlap = set(minted["shard0"]) & set(minted["shard1"])
    print(f"shard0 minted: {minted['shard0']}")
    print(f"shard1 minted: {minted['shard1']}")
    if overlap:
        print(f"❌ overlapping ids after fix: {sorted(overlap)}")
        raise SystemExit(1)
    cleanup([(s, i) for s, ids_ in minted.items() for i in ids_])

    state = read_router_state()
    state["sequences_fixed"] = True
    write_router_state(state)
    log.info("sequences interleaved; insert path unfrozen (router_state.sequences_fixed)")
    print("\n✅ 20 inserts, disjoint parity, zero collisions — and the id spaces")
    print("   can never meet. (production answer: lab 13's Snowflake ids)")


if __name__ == "__main__":
    main()
