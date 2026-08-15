"""Thundering herd: one hot key expires, N readers arrive in the same instant.

The setup is honest: the key is genuinely absent from Redis (as after a TTL
expiry), the N readers are released by a barrier so they really do race, and
the metric is the count of SELECTs MySQL actually served — plus what the herd
did to client latency.

Three protection modes on the identical stampede:

    naive          everyone misses, everyone SELECTs. DB cost: N.
    singleflight   per-key Redis lock (SET NX PX): one winner refills, N-1
                   losers poll the key. DB cost: 1. Losers pay the refill
                   latency — they queue behind the winner.
    swr            stale-while-revalidate: the expired-but-still-present value
                   is served immediately while one reader refreshes in the
                   background slot. DB cost: 1, latency flat. Price: every
                   reader in that window knowingly got a stale value.

Writes herd_summary.json for report.py.
"""
import argparse
import json
import threading
import time

from cache_client import CacheClient
from common import LAB_DIR, connect_redis, log

SUMMARY = LAB_DIR / "herd_summary.json"
HOT_PID = 7

# A key only becomes a herd problem when the query behind it costs something —
# that is WHY it was cached. Model that cost explicitly (80ms, think a fat
# aggregate or a query on a box that is already busy): while the first miss is
# still inside its 80ms, every other reader also misses. Without this, a
# localhost SELECT refills the key so fast the herd can't form — which is
# itself a lesson: herd risk scales with refill latency.
DB_LATENCY_MS = 80


def pct(sorted_vals: list, p: float) -> float:
    return sorted_vals[min(len(sorted_vals) - 1, int(p / 100 * len(sorted_vals)))]


def stampede(mode: str, n_readers: int) -> dict:
    r = connect_redis()
    r.delete(f"p:{HOT_PID}", f"lock:{HOT_PID}", f"swr:{HOT_PID}", f"swrlock:{HOT_PID}")
    if mode == "swr":
        # The value physically survives its logical expiry — that carcass is
        # exactly what swr serves while one reader revalidates.
        r.setex(f"swr:{HOT_PID}", 300,
                json.dumps({"price": 10.0, "fresh_until": time.time() - 1}))
    r.close()

    clients = [CacheClient("ttl", ttl_s=30, name=f"c{i}", db_latency_ms=DB_LATENCY_MS)
               for i in range(n_readers)]
    for c in clients:      # warm pools OUTSIDE the measured window — a real app
        _ = c.db           # holds pooled conns; cold-connect noise isn't the lesson
        c.r.ping()
    barrier = threading.Barrier(n_readers)
    latencies: list[float] = [0.0] * n_readers

    def hit(i: int) -> None:
        c = clients[i]
        barrier.wait()
        t0 = time.perf_counter()
        if mode == "naive":
            c.read(HOT_PID)
        elif mode == "singleflight":
            c.read_singleflight(HOT_PID)
        else:
            c.read_swr(HOT_PID, fresh_s=1.0)
        latencies[i] = (time.perf_counter() - t0) * 1000

    threads = [threading.Thread(target=hit, args=(i,)) for i in range(n_readers)]
    t0 = time.perf_counter()
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    wall_ms = (time.perf_counter() - t0) * 1000

    db_reads = sum(c.db_reads for c in clients)
    for c in clients:
        c.close()
    lat = sorted(latencies)
    return {"db_reads": db_reads, "wall_ms": round(wall_ms, 1),
            "p50_ms": round(pct(lat, 50), 1), "p99_ms": round(pct(lat, 99), 1)}


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--readers", type=int, default=200)
    args = ap.parse_args()

    print(f"=== hot key expires, {args.readers} concurrent readers stampede ===\n")
    print(f"  {'mode':<14} {'DB reads':>8} {'amplification':>13} "
          f"{'p50 lat':>9} {'p99 lat':>9} {'wall':>8}")
    results = {}
    for mode in ("naive", "singleflight", "swr"):
        res = stampede(mode, args.readers)
        results[mode] = res
        print(f"  {mode:<14} {res['db_reads']:>8} {res['db_reads']:>12}x "
              f"{res['p50_ms']:>7}ms {res['p99_ms']:>7}ms {res['wall_ms']:>6}ms")

    SUMMARY.write_text(json.dumps({"readers": args.readers, **results}, indent=2))
    naive, sf = results["naive"]["db_reads"], results["singleflight"]["db_reads"]
    if naive < args.readers * 0.5:
        log.error("❌ naive herd did not materialize (%d DB reads) — no spike, no lesson", naive)
        raise SystemExit(1)
    if sf > 3:
        log.error("❌ singleflight leaked %d DB reads — the lock is decorative", sf)
        raise SystemExit(1)
    log.info("✅ herd: naive amplified the miss %dx; singleflight capped it at %d; "
             "swr also %d and with a flat p99 — the cost is admitted staleness",
             naive, sf, results["swr"]["db_reads"])


if __name__ == "__main__":
    main()
