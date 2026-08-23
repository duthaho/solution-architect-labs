"""One adapter interface over all six top-K contenders.

Contract: update(key, minute_no) per event (in-memory contenders may ignore
the minute), topk(k) -> [(key, count)] best-first, memory_bytes(), close().
Run as a script for a 10k-event smoke test through every adapter.
"""

import sys
from collections import Counter

import common
from sketches import CountMinSketch
from topk_stores import SpaceSaving, TopKHeap

REDIS_TOPK_KEY = "lab14:topk"
MYSQL_BATCH = 5000


class ExactDict:
    name = "exact"

    def __init__(self, k: int = common.TOP_K):
        self.k = k
        self.counts: Counter = Counter()

    def update(self, key: int, minute_no: int) -> None:
        self.counts[key] += 1

    def topk(self, k: int | None = None) -> list[tuple[int, int]]:
        # not most_common(): ties must break deterministically, by key
        return sorted(self.counts.items(), key=lambda kv: (-kv[1], kv[0]))[: k or self.k]

    def memory_bytes(self) -> int:
        return len(self.counts) * 100

    def close(self) -> None:
        pass


class CmsHeap:
    name = "cms"

    def __init__(self, k: int = common.TOP_K, width: int = 1 << 16, depth: int = 4,
                 conservative: bool = False):
        self.store = TopKHeap(k, CountMinSketch(width, depth, conservative))
        if conservative:
            self.name = "cms_cu"

    def update(self, key: int, minute_no: int) -> None:
        self.store.update(key)

    def topk(self, k: int | None = None) -> list[tuple[int, int]]:
        return self.store.topk(k)

    def memory_bytes(self) -> int:
        return self.store.sketch.memory_bytes()

    def close(self) -> None:
        pass


class SpaceSavingAdapter:
    name = "spacesaving"

    def __init__(self, k: int = common.TOP_K, m: int = 4096):
        self.k = k
        self.store = SpaceSaving(m)

    def update(self, key: int, minute_no: int) -> None:
        self.store.update(key)

    def topk(self, k: int | None = None) -> list[tuple[int, int]]:
        return self.store.topk(k or self.k)

    def memory_bytes(self) -> int:
        return self.store.memory_bytes()

    def close(self) -> None:
        pass


class RedisTopK:
    name = "redis_topk"

    def __init__(self, k: int = common.TOP_K, key: str = REDIS_TOPK_KEY):
        self.k = k
        self.key = key
        self.r = common.redis_client()
        self.r.delete(key)
        # width/depth per HeavyKeeper defaults scaled to k
        self.r.execute_command("TOPK.RESERVE", key, k, k * 8, 7, 0.9)
        self.buf: list[int] = []

    def update(self, key: int, minute_no: int) -> None:
        self.buf.append(key)
        if len(self.buf) >= MYSQL_BATCH:
            self.flush()

    def flush(self) -> None:
        if self.buf:
            self.r.execute_command("TOPK.ADD", self.key, *self.buf)
            self.buf.clear()

    def topk(self, k: int | None = None) -> list[tuple[int, int]]:
        self.flush()
        raw = self.r.execute_command("TOPK.LIST", self.key, "WITHCOUNT")
        pairs = [(int(raw[i]), int(raw[i + 1])) for i in range(0, len(raw), 2)]
        return pairs[: k or self.k]

    def memory_bytes(self) -> int:
        self.flush()
        return int(self.r.memory_usage(self.key) or 0)

    def close(self) -> None:
        self.flush()
        self.r.close()


class MysqlRollup:
    name = "mysql_rollup"

    def __init__(self, k: int = common.TOP_K):
        self.k = k
        self.conn = common.connect(autocommit=False)
        with self.conn.cursor() as cur:
            cur.execute("TRUNCATE TABLE views_minute")
        self.conn.commit()
        self.buf: list[tuple[int, int]] = []

    def update(self, key: int, minute_no: int) -> None:
        self.buf.append((minute_no, key))
        if len(self.buf) >= MYSQL_BATCH:
            self.flush()

    def flush(self) -> None:
        if not self.buf:
            return
        agg = Counter(self.buf)
        with self.conn.cursor() as cur:
            cur.executemany(
                "INSERT INTO views_minute (minute_no, product_id, cnt) "
                "VALUES (%s, %s, %s) "
                "ON DUPLICATE KEY UPDATE cnt = cnt + VALUES(cnt)",
                [(m, p, c) for (m, p), c in agg.items()],
            )
        self.conn.commit()
        self.buf.clear()

    def topk(self, k: int | None = None) -> list[tuple[int, int]]:
        self.flush()
        with self.conn.cursor() as cur:
            cur.execute(
                "SELECT product_id, SUM(cnt) AS total FROM views_minute "
                "GROUP BY product_id ORDER BY total DESC, product_id LIMIT %s",
                (k or self.k,),
            )
            return [(int(p), int(c)) for p, c in cur.fetchall()]

    def memory_bytes(self) -> int:
        self.flush()
        with self.conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM views_minute")
            (rows,) = cur.fetchone()
        return int(rows) * 60  # ~row footprint incl. PK + secondary index

    def close(self) -> None:
        self.flush()
        self.conn.close()


def build_all(k: int = common.TOP_K, cms_width: int = 1 << 16) -> list:
    return [
        ExactDict(k),
        CmsHeap(k, cms_width),
        CmsHeap(k, cms_width, conservative=True),
        SpaceSavingAdapter(k),
        RedisTopK(k),
        MysqlRollup(k),
    ]


def _smoke() -> int:
    n = 10_000
    contenders = build_all(k=10, cms_width=1 << 14)
    for i, key in enumerate(common.zipf_stream(n_events=n, n_keys=5_000)):
        minute = common.minute_of(i, n)
        for c in contenders:
            c.update(key, minute)
    failures = []
    for c in contenders:
        top = c.topk(10)
        if len(top) != 10 or any(cnt <= 0 for _, cnt in top):
            failures.append(f"{c.name}: implausible top-10 {top[:3]}...")
        common.log.info("%-12s top3=%s mem=%dB", c.name, top[:3], c.memory_bytes())
        c.close()
    for f in failures:
        common.log.error("smoke: %s", f)
    if failures:
        return 1
    common.log.info("contenders smoke OK: all six answered top-10")
    return 0


if __name__ == "__main__":
    sys.exit(_smoke())
