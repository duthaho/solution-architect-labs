"""The cache strategies, with named interleaving hooks — the lab's core.

Every strategy implements the same two-method interface:

    read(pid)  -> price     the app's read path (cache-aside on a miss)
    write(pid, price)       the app's write path (DB commit + invalidation)

and every step of both paths announces itself through `hooks.fire(name)`.
In normal operation the hooks are no-ops. Under race.py, a hook becomes a
rendezvous point: the thread parks there until the driver says go. That is
the whole determinism trick — the classic "read-then-pause-then-set" race is
not provoked by hammering the system 10k times, it is *scheduled*, step by
step, and reproduces on every single run.

The hook names, in path order:

    read path:   read_start -> [cache hit -> served_from_cache]
                            -> cache_miss -> after_db_read -> before_cache_set
                            -> after_cache_set
    write path:  write_start -> after_db_write -> before_invalidate
                            -> after_invalidate

Strategies (README §3 has the deep dive):

    ttl        SETEX on miss, writes touch ONLY the DB. Staleness = up to TTL.
               The honest baseline: it never claims freshness, so it never
               lies about it.
    delete     ttl + DELETE the key after every DB write. Looks airtight,
               loses to one interleaving (drill-stale-set): a reader that read
               the DB *before* the write can SET the old value *after* the
               DELETE — stale until TTL, deterministically.
    versioned  writer INCRs a per-key version pointer in Redis; values live
               under p:{id}:v{n}. A late stale SET lands on a version nobody
               will ever read again (drill-versioned proves it, same exact
               interleaving as drill-stale-set).
    cdc        read path identical to ttl; NO app-side invalidation at all.
               invalidator.py tails the MySQL binlog via Debezium and deletes
               keys for rows that actually committed. Staleness = pipeline
               lag, measured, and it survives app-code bugs and out-of-band
               writes because invalidation is driven by the binlog, not by
               whoever remembered to call delete().

Instrumentation: every client counts db_reads (herd.py's metric) and journals
reads/writes (auditor.py's ground truth) when journaling=True.
"""
import json
import threading
import time

from common import (READ_JOURNAL, TABLE, WRITE_JOURNAL, connect_mysql,
                    connect_redis, journal, now_ms)

DEFAULT_TTL_S = int(__import__("os").environ.get("CACHE_TTL_S", "30"))


class Hooks:
    """No-op hook sink. race.py subclasses this to turn named points into
    thread rendezvous. Keeping the base class trivial keeps the hot path
    honest: one virtual call per step, zero synchronization."""

    def fire(self, name: str) -> None:
        pass


class CacheClient:
    """One client = one MySQL conn + one Redis conn + one strategy's methods.
    Not thread-safe across calls by design — race.py gives each simulated
    process its own client, exactly like real app instances."""

    def __init__(self, strategy: str, hooks: Hooks | None = None,
                 ttl_s: int = DEFAULT_TTL_S, journaling: bool = False,
                 name: str = "client", db_latency_ms: int = 0):
        assert strategy in ("ttl", "delete", "versioned", "cdc"), strategy
        self.strategy = strategy
        self.hooks = hooks or Hooks()
        self.ttl_s = ttl_s
        self.db_latency_ms = db_latency_ms   # herd.py: model a query that costs something
        self.journaling = journaling
        self.name = name
        self._db = None        # lazy: 200 herd clients must not open 200 MySQL
        self.r = connect_redis()   # conns when only the miss path needs one
        self.db_reads = 0      # herd.py's metric: how many misses hit MySQL
        self.cache_hits = 0

    @property
    def db(self):
        if self._db is None:
            self._db = connect_mysql()
        return self._db

    # ------------------------------------------------------------ primitives

    def _db_read(self, pid: int) -> float:
        self.db_reads += 1
        if self.db_latency_ms:
            time.sleep(self.db_latency_ms / 1000)
        with self.db.cursor() as cur:
            cur.execute(f"SELECT price FROM {TABLE} WHERE id=%s", (pid,))
            row = cur.fetchone()
        if row is None:
            raise KeyError(f"product {pid} not found")
        return float(row[0])

    def _db_write(self, pid: int, price: float) -> None:
        with self.db.cursor() as cur:
            cur.execute(
                f"UPDATE {TABLE} SET price=%s, version=version+1 WHERE id=%s",
                (price, pid))
        # Journal AFTER commit (autocommit): the journal must only ever
        # contain prices a reader could legitimately observe.
        if self.journaling:
            journal(WRITE_JOURNAL, {"ts": now_ms(), "id": pid, "price": price,
                                    "strategy": self.strategy})

    def _serve(self, pid: int, price: float, source: str) -> float:
        if source == "cache":
            self.cache_hits += 1
        if self.journaling:
            journal(READ_JOURNAL, {"ts": now_ms(), "id": pid, "price": price,
                                   "source": source, "strategy": self.strategy})
        return price

    # ------------------------------------------------- ttl / delete / cdc read
    # (identical read path: plain key, cache-aside, SETEX on miss)

    def _read_plain(self, pid: int) -> float:
        key = f"p:{pid}"
        self.hooks.fire("read_start")
        cached = self.r.get(key)
        if cached is not None:
            self.hooks.fire("served_from_cache")
            return self._serve(pid, float(cached), "cache")
        self.hooks.fire("cache_miss")
        price = self._db_read(pid)
        self.hooks.fire("after_db_read")        # <-- the race parks HERE
        self.hooks.fire("before_cache_set")
        self.r.setex(key, self.ttl_s, repr(price))
        self.hooks.fire("after_cache_set")
        return self._serve(pid, price, "db")

    # ------------------------------------------------------------- versioned

    def _read_versioned(self, pid: int) -> float:
        vkey = f"v:{pid}"
        self.hooks.fire("read_start")
        ver = self.r.get(vkey)
        if ver is None:
            # First touch: initialize the pointer with NX so a racing reader
            # can never move it — only writers (INCR) advance versions.
            self.r.set(vkey, 1, nx=True)
            ver = self.r.get(vkey)
        key = f"p:{pid}:v{ver}"
        cached = self.r.get(key)
        if cached is not None:
            self.hooks.fire("served_from_cache")
            return self._serve(pid, float(cached), "cache")
        self.hooks.fire("cache_miss")
        price = self._db_read(pid)
        self.hooks.fire("after_db_read")        # <-- same park point as _read_plain
        self.hooks.fire("before_cache_set")
        # A stale SET lands under the version we read BEFORE the DB read.
        # If a writer committed since, it also INCRed v:{pid} past us: this
        # key is already unreachable. Stale data nobody can address == no race.
        self.r.setex(key, self.ttl_s, repr(price))
        self.hooks.fire("after_cache_set")
        return self._serve(pid, price, "db")

    # ---------------------------------------------------------------- writes

    def _write_ttl(self, pid: int, price: float) -> None:
        self.hooks.fire("write_start")
        self._db_write(pid, price)
        self.hooks.fire("after_db_write")       # ttl/cdc: no invalidation step

    def _write_delete(self, pid: int, price: float) -> None:
        self.hooks.fire("write_start")
        self._db_write(pid, price)
        self.hooks.fire("after_db_write")
        self.hooks.fire("before_invalidate")
        self.r.delete(f"p:{pid}")               # delete-on-write, after commit
        self.hooks.fire("after_invalidate")

    def _write_versioned(self, pid: int, price: float) -> None:
        self.hooks.fire("write_start")
        self._db_write(pid, price)
        self.hooks.fire("after_db_write")
        self.hooks.fire("before_invalidate")
        self.r.incr(f"v:{pid}")                 # the "invalidation" is a pointer bump
        self.hooks.fire("after_invalidate")

    # ------------------------------------------------------------- dispatch

    def read(self, pid: int) -> float:
        if self.strategy == "versioned":
            return self._read_versioned(pid)
        return self._read_plain(pid)            # ttl, delete, cdc

    def write(self, pid: int, price: float) -> None:
        if self.strategy == "versioned":
            self._write_versioned(pid, price)
        elif self.strategy == "delete":
            self._write_delete(pid, price)
        else:
            self._write_ttl(pid, price)         # ttl, cdc: DB only

    # ------------------------------------------------ herd protection (herd.py)

    def read_singleflight(self, pid: int, lock_ms: int = 2000) -> float:
        """Cache-aside + per-key Redis lock: on a miss, ONE process (across
        all processes, not just threads — the lock lives in Redis) refills;
        the rest poll the key. Lock has a TTL so a crashed winner can't wedge
        the key; losers fall through to the DB after a bounded wait, so the
        protection degrades to 'naive', never to 'unavailable'."""
        key, lock = f"p:{pid}", f"lock:{pid}"
        cached = self.r.get(key)
        if cached is not None:
            return self._serve(pid, float(cached), "cache")
        if self.r.set(lock, self.name, nx=True, px=lock_ms):
            try:
                price = self._db_read(pid)
                self.r.setex(key, self.ttl_s, repr(price))
                return self._serve(pid, price, "db")
            finally:
                self.r.delete(lock)
        deadline = time.time() + lock_ms / 1000
        while time.time() < deadline:
            cached = self.r.get(key)
            if cached is not None:
                return self._serve(pid, float(cached), "cache-waited")
            time.sleep(0.005)
        return self._serve(pid, self._db_read(pid), "db-lock-timeout")

    def read_swr(self, pid: int, fresh_s: float = 1.0) -> float:
        """Stale-while-revalidate: the payload carries its own logical expiry;
        the physical TTL is much longer. Past logical expiry, the first reader
        to win the revalidate lock refreshes; everyone else serves the stale
        value NOW. Herd -> 1 DB read, latency flat; cost: bounded, admitted
        staleness (fresh_s + refresh time)."""
        key, lock = f"swr:{pid}", f"swrlock:{pid}"
        raw = self.r.get(key)
        if raw is not None:
            obj = json.loads(raw)
            if time.time() < obj["fresh_until"]:
                return self._serve(pid, obj["price"], "cache")
            if self.r.set(lock, self.name, nx=True, px=2000):
                try:
                    price = self._db_read(pid)
                    self.r.setex(key, self.ttl_s, json.dumps(
                        {"price": price, "fresh_until": time.time() + fresh_s}))
                    return self._serve(pid, price, "db-revalidate")
                finally:
                    self.r.delete(lock)
            return self._serve(pid, obj["price"], "cache-stale")
        # cold miss: no stale value to serve, behave like singleflight
        if self.r.set(lock, self.name, nx=True, px=2000):
            try:
                price = self._db_read(pid)
                self.r.setex(key, self.ttl_s, json.dumps(
                    {"price": price, "fresh_until": time.time() + fresh_s}))
                return self._serve(pid, price, "db")
            finally:
                self.r.delete(lock)
        deadline = time.time() + 2.0
        while time.time() < deadline:
            raw = self.r.get(key)
            if raw is not None:
                return self._serve(pid, json.loads(raw)["price"], "cache-waited")
            time.sleep(0.005)
        return self._serve(pid, self._db_read(pid), "db-lock-timeout")

    def close(self) -> None:
        try:
            if self._db is not None:
                self._db.close()
        except Exception:
            pass
        try:
            self.r.close()
        except Exception:
            pass
