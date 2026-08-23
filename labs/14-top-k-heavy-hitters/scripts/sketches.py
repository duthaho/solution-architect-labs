"""Count-Min Sketch, vanilla and conservative-update.

Run as a script for the selftest: one-sided error, bit-identical merge of
halves vs the full stream, conservative update strictly tighter, and the
merge non-linearity of conservative update. SELFTEST_BREAK=1 corrupts one
counter and must make the selftest FAIL — proof it is not vacuous.
"""

import hashlib
import os
import sys
from array import array

import common


class CountMinSketch:
    def __init__(
        self,
        width: int,
        depth: int = 4,
        conservative: bool = False,
        sketch_seed: int = common.SKETCH_SEED,
    ):
        self.width = width
        self.depth = depth
        self.conservative = conservative
        self.sketch_seed = sketch_seed
        self.params = common.hash_params(depth, sketch_seed)
        self.rows = [array("Q", [0] * width) for _ in range(depth)]
        self.total = 0

    def raw_hashes(self, key: int) -> list[int]:
        """Width-independent hash values — shareable across sketches built
        from the same seed, so a multi-width sweep hashes each event once."""
        return [(a * key + b) % common.MERSENNE_P for a, b in self.params]

    def update(self, key: int, count: int = 1) -> None:
        self.update_hashed(self.raw_hashes(key), count)

    def update_hashed(self, hashes: list[int], count: int = 1) -> None:
        self.total += count
        if not self.conservative:
            for row, h in zip(self.rows, hashes):
                row[h % self.width] += count
            return
        idx = [h % self.width for h in hashes]
        cells = [row[i] for row, i in zip(self.rows, idx)]
        target = min(cells) + count
        for row, i, cur in zip(self.rows, idx, cells):
            if cur < target:
                row[i] = target

    def estimate(self, key: int) -> int:
        return self.estimate_hashed(self.raw_hashes(key))

    def estimate_hashed(self, hashes: list[int]) -> int:
        return min(row[h % self.width] for row, h in zip(self.rows, hashes))

    def merge(self, other: "CountMinSketch") -> "CountMinSketch":
        if (
            self.width != other.width
            or self.depth != other.depth
            or self.sketch_seed != other.sketch_seed
        ):
            raise ValueError("cannot merge sketches with different shape or seeds")
        out = CountMinSketch(self.width, self.depth, self.conservative, self.sketch_seed)
        for orow, a, b in zip(out.rows, self.rows, other.rows):
            for i in range(self.width):
                orow[i] = a[i] + b[i]
        out.total = self.total + other.total
        return out

    def checksum(self) -> str:
        h = hashlib.sha256()
        for row in self.rows:
            h.update(row.tobytes())
        return h.hexdigest()

    def memory_bytes(self) -> int:
        return self.width * self.depth * 8


def _selftest() -> int:
    stream = list(common.zipf_stream(n_events=200_000, n_keys=50_000))
    truth: dict[int, int] = {}
    for k in stream:
        truth[k] = truth.get(k, 0) + 1

    cms = CountMinSketch(width=4096)
    cu = CountMinSketch(width=4096, conservative=True)
    half_a = CountMinSketch(width=4096)
    half_b = CountMinSketch(width=4096)
    cu_half_a = CountMinSketch(width=4096, conservative=True)
    cu_half_b = CountMinSketch(width=4096, conservative=True)
    mid = len(stream) // 2
    for i, k in enumerate(stream):
        cms.update(k)
        cu.update(k)
        (half_a if i < mid else half_b).update(k)
        (cu_half_a if i < mid else cu_half_b).update(k)

    if os.environ.get("SELFTEST_BREAK") == "1":
        cms.rows[0][0] = 0  # corrupt: one-sided error must now be violated

    failures = []

    below = sum(1 for k, f in truth.items() if cms.estimate(k) < f)
    if below:
        failures.append(f"one-sided error violated for {below} keys")

    merged = half_a.merge(half_b)
    if merged.checksum() != cms.checksum():
        failures.append("merged halves != full-stream sketch (linearity broken)")

    cms_err = sum(cms.estimate(k) - f for k, f in truth.items())
    cu_err = sum(cu.estimate(k) - f for k, f in truth.items())
    if not cu_err < cms_err:
        failures.append(f"conservative update not tighter ({cu_err} vs {cms_err})")

    cu_merged = cu_half_a.merge(cu_half_b)
    if cu_merged.checksum() == cu.checksum():
        failures.append("CU merge unexpectedly bit-identical — non-linearity not shown")

    for f in failures:
        common.log.error("selftest: %s", f)
    if failures:
        common.log.error("sketches selftest FAILED (%d checks)", len(failures))
        return 1
    common.log.info(
        "sketches selftest OK: one-sided error, merge identity, CU tighter "
        "(%d vs %d overcount), CU merge non-linear", cu_err, cms_err
    )
    return 0


if __name__ == "__main__":
    sys.exit(_selftest())
