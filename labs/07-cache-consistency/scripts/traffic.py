"""Soak traffic: journaling readers + writers against one strategy.

A HOT set of products takes all the writes and most of the reads — staleness
only shows where reads chase writes, so the workload concentrates there on
purpose. Every committed price and every served price is journaled;
auditor.py turns the two journals into measured staleness. Runs for
--duration seconds, or until SIGTERM when run in the background (drill-cdc).
"""
import argparse
import random
import signal
import threading
import time

from cache_client import CacheClient
from common import N_PRODUCTS, log

HOT = 50          # products 1..HOT take every write and 90% of reads
stop = threading.Event()


def writer_loop(strategy: str, ttl_s: int, rate: float, rng: random.Random) -> None:
    c = CacheClient(strategy, ttl_s=ttl_s, journaling=True, name="writer")
    while not stop.is_set():
        c.write(rng.randrange(1, HOT + 1), round(rng.uniform(1, 500), 2))
        time.sleep(rng.expovariate(rate))
    c.close()


def reader_loop(strategy: str, ttl_s: int, rate: float, rng: random.Random) -> None:
    c = CacheClient(strategy, ttl_s=ttl_s, journaling=True, name="reader")
    while not stop.is_set():
        pid = rng.randrange(1, HOT + 1) if rng.random() < 0.9 \
            else rng.randrange(1, N_PRODUCTS + 1)
        try:
            c.read(pid)
        except Exception as e:            # noqa: BLE001 — soak must survive blips
            log.warning("read failed: %s", e)
        time.sleep(rng.expovariate(rate))
    c.close()


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--strategy", required=True, choices=["ttl", "delete", "versioned", "cdc"])
    ap.add_argument("--duration", type=float, default=20.0,
                    help="seconds; 0 = run until SIGTERM (background soak)")
    ap.add_argument("--readers", type=int, default=8)
    ap.add_argument("--writers", type=int, default=2)
    ap.add_argument("--read-rate", type=float, default=25.0, help="reads/s per reader")
    ap.add_argument("--write-rate", type=float, default=5.0, help="writes/s per writer")
    ap.add_argument("--ttl", type=int, default=30)
    args = ap.parse_args()

    signal.signal(signal.SIGTERM, lambda *_: stop.set())
    signal.signal(signal.SIGINT, lambda *_: stop.set())

    threads = [threading.Thread(target=writer_loop,
                                args=(args.strategy, args.ttl, args.write_rate, random.Random(1000 + i)))
               for i in range(args.writers)]
    threads += [threading.Thread(target=reader_loop,
                                 args=(args.strategy, args.ttl, args.read_rate, random.Random(i)))
                for i in range(args.readers)]
    for t in threads:
        t.start()
    log.info("soak: strategy=%s ttl=%ds readers=%d writers=%d duration=%s",
             args.strategy, args.ttl, args.readers, args.writers,
             f"{args.duration}s" if args.duration else "until SIGTERM")

    if args.duration:
        stop.wait(args.duration)
        stop.set()
    else:
        while not stop.wait(1):
            pass
    for t in threads:
        t.join()
    log.info("soak done (strategy=%s)", args.strategy)


if __name__ == "__main__":
    main()
