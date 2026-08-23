"""Space-Saving (Metwally et al. 2005) and the CMS-backed top-K heap.

Run as a script for the selftest: the Space-Saving guarantee (every key
with true count > N/m is present) and heap top-K == exact top-K when the
CMS is generously sized.
"""

import heapq
import sys

import common
from sketches import CountMinSketch


class SpaceSaving:
    """m counters; a new key evicts the min counter and inherits its count."""

    def __init__(self, m: int):
        self.m = m
        self.counts: dict[int, int] = {}
        self.errors: dict[int, int] = {}
        self.total = 0
        # lazy min-heap over counts: a linear min() scan per eviction would
        # be O(m) on every unseen key once full
        self.heap: list[tuple[int, int]] = []

    def update(self, key: int, count: int = 1) -> None:
        self.total += count
        if key in self.counts:
            c = self.counts[key] + count
            self.counts[key] = c
            heapq.heappush(self.heap, (c, key))
        elif len(self.counts) < self.m:
            self.counts[key] = count
            self.errors[key] = 0
            heapq.heappush(self.heap, (count, key))
        else:
            self._prune()
            floor, victim = heapq.heappop(self.heap)
            del self.counts[victim]
            del self.errors[victim]
            self.counts[key] = floor + count
            self.errors[key] = floor
            heapq.heappush(self.heap, (floor + count, key))
        if len(self.heap) > 16 * self.m:
            self.heap = [(c, k) for k, c in self.counts.items()]
            heapq.heapify(self.heap)

    def _prune(self) -> None:
        while self.heap and self.counts.get(self.heap[0][1]) != self.heap[0][0]:
            heapq.heappop(self.heap)

    def estimate(self, key: int) -> int:
        return self.counts.get(key, 0)

    def topk(self, k: int) -> list[tuple[int, int]]:
        return sorted(self.counts.items(), key=lambda kv: -kv[1])[:k]

    def memory_bytes(self) -> int:
        # two dict slots of (int key, int count) at CPython's ~50B/entry
        return len(self.counts) * 100


class TopKHeap:
    """Size-k candidate set + lazy min-heap maintained alongside a sketch.

    Stale heap entries are pruned on demand instead of re-heapifying per
    update — hot keys update their estimate on every event, so an in-place
    heap rewrite would be O(k) on the hottest path.
    """

    def __init__(self, k: int, sketch: CountMinSketch):
        self.k = k
        self.sketch = sketch
        self.est: dict[int, int] = {}
        self.heap: list[tuple[int, int]] = []

    def update(self, key: int, count: int = 1) -> None:
        self.sketch.update(key, count)
        est = self.sketch.estimate(key)
        if key in self.est or len(self.est) < self.k:
            self.est[key] = est
            heapq.heappush(self.heap, (est, key))
        else:
            self._prune()
            if est > self.heap[0][0]:
                _, victim = heapq.heappop(self.heap)
                del self.est[victim]
                self.est[key] = est
                heapq.heappush(self.heap, (est, key))
        if len(self.heap) > 8 * self.k:
            self.heap = [(e, m) for m, e in self.est.items()]
            heapq.heapify(self.heap)

    def _prune(self) -> None:
        while self.heap and self.est.get(self.heap[0][1]) != self.heap[0][0]:
            heapq.heappop(self.heap)

    def topk(self, k: int | None = None) -> list[tuple[int, int]]:
        return sorted(self.est.items(), key=lambda kv: (-kv[1], kv[0]))[: k or self.k]


def _selftest() -> int:
    n_events, n_keys, k = 200_000, 50_000, 50
    truth: dict[int, int] = {}
    ss = SpaceSaving(m=2000)
    heap = TopKHeap(k, CountMinSketch(width=1 << 16))
    for key in common.zipf_stream(n_events=n_events, n_keys=n_keys):
        truth[key] = truth.get(key, 0) + 1
        ss.update(key)
        heap.update(key)

    failures = []

    threshold = n_events / ss.m
    guaranteed = {key for key, f in truth.items() if f > threshold}
    missing = guaranteed - set(ss.counts)
    if missing:
        failures.append(f"space-saving guarantee broken: {len(missing)} keys > N/m absent")
    over = sum(1 for key in ss.counts if ss.counts[key] < truth.get(key, 0))
    if over:
        failures.append(f"space-saving underestimates {over} tracked keys")

    exact_top = sorted(truth.items(), key=lambda kv: -kv[1])[:k]
    if {key for key, _ in heap.topk()} != {key for key, _ in exact_top}:
        failures.append("generous CMS heap top-k != exact top-k")

    for f in failures:
        common.log.error("selftest: %s", f)
    if failures:
        common.log.error("topk_stores selftest FAILED (%d checks)", len(failures))
        return 1
    common.log.info(
        "topk_stores selftest OK: %d guaranteed keys all present, "
        "heap top-%d matches exact", len(guaranteed), k
    )
    return 0


if __name__ == "__main__":
    sys.exit(_selftest())
