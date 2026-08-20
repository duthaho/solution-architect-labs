"""Generation bench: Snowflake vs UUIDv4 vs UUIDv7 vs AUTO_INCREMENT vs INCR.

BENCH_WORKERS processes per scheme, each emitting its quota; every emission
is stamped with time_ns so the per-scheme streams can be merged in arrival
order. Reported per scheme:

  ops/s, p50/p95 per-op latency, id width in bits, canonical text length,
  and the merged-stream **normalized rank displacement** — mean distance
  between each id's arrival position and its position in sorted order,
  divided by stream length. ~33% = random placement (UUIDv4: an insert lands
  anywhere in the keyspace); ~0% = time-ordered (Snowflake, UUIDv7: an
  insert lands near the end). This, not adjacent inversions, is the B-tree
  locality story — ids emitted within the same millisecond may shuffle
  locally (uuid7's random bits) without hurting locality.

Exit code gates only structural facts (throughput is hardware, narrated in
the README, never asserted):
  - every scheme's ids are unique within the run
  - snowflake and uuid7 displacement is under a third of uuid4's

Workers write bench_<scheme>_w<n>.jsonl (not ids_*: bench streams are not
verify's input). Requires `make bootstrap` (autoinc table, lease slots).
"""

import multiprocessing as mp
import os
import sys
import time

import common
import snowflake

WORKERS = int(os.environ.get("BENCH_WORKERS", "4"))
N_INPROC = int(os.environ.get("BENCH_N_INPROC", "20000"))
N_AUTOINC = int(os.environ.get("BENCH_N_AUTOINC", "1500"))
N_REDIS = int(os.environ.get("BENCH_N_REDIS", "5000"))

SCHEMES = [
    ("snowflake", N_INPROC, 63, "MySQL lease per worker (claim + heartbeat)"),
    ("uuid7", N_INPROC, 128, "none"),
    ("uuid4", N_INPROC, 128, "none"),
    ("autoinc", N_AUTOINC, 64, "DB round-trip per id"),
    ("redis_incr", N_REDIS, 64, "Redis round-trip per id"),
]


def bench_path(scheme: str, w: int):
    return common.LAB_DIR / f"bench_{scheme}_w{w}.jsonl"


def make_maker(scheme: str, conn_holder: dict):
    import alternatives

    if scheme == "snowflake":
        import lease as lease_mod

        conn = common.connect(autocommit=True)
        conn_holder["cleanup"] = lambda: (lse.release(conn), conn.close())
        lse = lease_mod.Lease()
        if lse.claim(conn) is None:
            raise RuntimeError("no free worker-id lease")
        gen = lease_mod.LeasedGenerator(lse)
        conn_holder["lease"] = (lse, conn)
        return lambda: (gen.next_id()[0],)
    if scheme == "uuid4":
        return lambda: (alternatives.make_uuid4()[0],)
    if scheme == "uuid7":
        return lambda: (alternatives.make_uuid7()[0],)
    if scheme == "autoinc":
        conn = common.connect()
        conn_holder["cleanup"] = conn.close
        return lambda m=alternatives.AutoincMaker(conn): (m()[0],)
    if scheme == "redis_incr":
        client = common.redis_client()
        return lambda m=alternatives.RedisIncrMaker(client): (m()[0],)
    raise ValueError(scheme)


def worker(scheme: str, w: int, n: int) -> None:
    holder: dict = {}
    maker = make_maker(scheme, holder)
    lease_conn = holder.get("lease")
    out = []
    # Time-based heartbeats: a count-based cadence would couple lease
    # survival (and thus the bench's exit code) to interpreter speed.
    hb_interval = common.LEASE_TTL_MS // 3
    next_hb = common.now_ms() + hb_interval
    for _ in range(n):
        t0 = time.monotonic_ns()
        (id_,) = maker()
        t1 = time.monotonic_ns()
        out.append((time.time_ns(), id_, t1 - t0))
        if lease_conn and common.now_ms() >= next_hb:
            lse, conn = lease_conn
            lse.heartbeat(conn)
            next_hb = common.now_ms() + hb_interval
    with bench_path(scheme, w).open("w") as f:
        for t, id_, dt in out:
            f.write(f'{{"t": {t}, "id": {id_}, "ns": {dt}}}\n')
    if "cleanup" in holder:
        try:
            holder["cleanup"]()
        except Exception:
            pass


def canonical_len(scheme: str, id_: int) -> int:
    if scheme in ("uuid4", "uuid7"):
        return 36  # canonical hyphenated form
    return len(str(id_))


def run_scheme(scheme: str, n: int) -> dict:
    for w in range(WORKERS):
        bench_path(scheme, w).unlink(missing_ok=True)
    procs = [mp.Process(target=worker, args=(scheme, w, n)) for w in range(WORKERS)]
    for p in procs:
        p.start()
    for p in procs:
        p.join()
    if any(p.exitcode != 0 for p in procs):
        raise RuntimeError(f"{scheme}: a bench worker failed")

    stream = []
    lat = []
    for w in range(WORKERS):
        for r in common.read_jsonl(bench_path(scheme, w)):
            stream.append((r["t"], r["id"]))
            lat.append(r["ns"] / 1000)  # µs
    stream.sort()  # merge by arrival time
    ids = [id_ for _, id_ in stream]
    # normalized rank displacement: |arrival position - sorted position| / N
    order = sorted(range(len(ids)), key=ids.__getitem__)
    sorted_rank = [0] * len(ids)
    for rank, idx in enumerate(order):
        sorted_rank[idx] = rank
    disp = sum(abs(i - r) for i, r in enumerate(sorted_rank)) / (len(ids) ** 2)
    span_s = (stream[-1][0] - stream[0][0]) / 1e9 or 1e-9
    p50, p95 = common.percentiles(lat)
    return {
        "ids": len(ids),
        "dupes": len(ids) - len(set(ids)),
        "ops_s": len(ids) / span_s,
        "p50_us": p50,
        "p95_us": p95,
        "disp": disp,
        "text_len": canonical_len(scheme, ids[0]),
    }


def main() -> int:
    results = {}
    for scheme, n, bits, coord in SCHEMES:
        common.log.info("bench: %s (%d workers x %d)", scheme, WORKERS, n)
        r = run_scheme(scheme, n)
        r["bits"] = bits
        r["coord"] = coord
        results[scheme] = r

    print(f"\n{'scheme':11} {'ids':>7} {'ops/s':>9} {'p50 µs':>8} {'p95 µs':>8} "
          f"{'bits':>5} {'text':>5} {'disp%':>6}  coordination")
    for scheme, _, _, _ in SCHEMES:
        r = results[scheme]
        print(f"{scheme:11} {r['ids']:>7} {r['ops_s']:>9.0f} {r['p50_us']:>8.2f} "
              f"{r['p95_us']:>8.2f} {r['bits']:>5} {r['text_len']:>5} "
              f"{100 * r['disp']:>6.2f}  {r['coord']}")
    print()

    ok = True
    for scheme, r in results.items():
        if r["dupes"]:
            common.log.error("%s produced %d duplicate ids", scheme, r["dupes"])
            ok = False
    u4 = results["uuid4"]["disp"]
    for sortable in ("snowflake", "uuid7"):
        if results[sortable]["disp"] >= u4 / 3:
            common.log.error("%s displacement %.4f not well below uuid4's %.4f",
                             sortable, results[sortable]["disp"], u4)
            ok = False
    if ok:
        common.log.info("bench: all schemes unique; sortable schemes sort — exit 0")
        return 0
    return 1


if __name__ == "__main__":
    sys.exit(main())
