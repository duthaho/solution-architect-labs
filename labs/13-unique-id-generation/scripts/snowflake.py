"""Snowflake-style 64-bit IDs: 41-bit ms timestamp | 10-bit worker | 12-bit seq.

Two generators live here:

- ``NaiveGenerator`` trusts the clock and wraps the sequence — the bug. A
  backwards clock jump makes it re-traverse (ts, seq) pairs it already issued;
  >4096 draws in one ms make the sequence wrap in place. Both emit duplicates.
- ``Generator`` is the hardened one. Policy for a backwards clock:
  ``error`` (refuse), ``wait`` (sleep out regressions up to WAIT_MAX_MS,
  refuse beyond), ``hold`` (keep issuing at last_ts from the remaining
  sequence, spilling forward only when it exhausts). Sequence exhaustion
  always spins to the next millisecond. Output is strictly monotonic.

Clocks are injectable (any ``() -> unix_ms`` callable) — that is what makes
every drill in this lab deterministic. ``ScriptedClock`` replays a fixed
step list; ``OffsetClock`` is real time plus a mutable offset (so ``wait``
provably terminates: real time keeps flowing underneath the offset).

Run this file directly for the selftest (``SELFTEST_BREAK=1`` proves the
selftest is non-vacuous by mis-composing the layout and expecting a catch).
"""

import os
import sys
import time

import common

EPOCH_MS = 1_704_067_200_000  # 2024-01-01T00:00:00Z — a custom epoch buys 41 bits ~69 years
TS_BITS = 41
WORKER_BITS = 10
SEQ_BITS = 12

MAX_TS = (1 << TS_BITS) - 1
MAX_WORKER = (1 << WORKER_BITS) - 1
MAX_SEQ = (1 << SEQ_BITS) - 1

WAIT_MAX_MS = int(os.environ.get("WAIT_MAX_MS", "500"))

POLICIES = ("error", "wait", "hold")


class ClockBackwardsError(RuntimeError):
    """The clock ran backwards further than the policy tolerates."""


def encode(ts_ms: int, worker: int, seq: int) -> int:
    assert 0 <= ts_ms <= MAX_TS and 0 <= worker <= MAX_WORKER and 0 <= seq <= MAX_SEQ
    return (ts_ms << (WORKER_BITS + SEQ_BITS)) | (worker << SEQ_BITS) | seq


def decode(id_: int) -> tuple[int, int, int]:
    """Return (ts_ms since custom epoch, worker, seq)."""
    return (
        id_ >> (WORKER_BITS + SEQ_BITS),
        (id_ >> SEQ_BITS) & MAX_WORKER,
        id_ & MAX_SEQ,
    )


def real_clock() -> int:
    return common.now_ms() - EPOCH_MS


class ScriptedClock:
    """Replays ``steps`` one per call; after exhaustion returns ``then()``."""

    def __init__(self, steps: list[int], then=None):
        self.steps = list(steps)
        self.i = 0
        self.then = then if then is not None else (lambda: self.steps[-1])

    def __call__(self) -> int:
        if self.i < len(self.steps):
            v = self.steps[self.i]
            self.i += 1
            return v
        return self.then()


class OffsetClock:
    """Real time plus a mutable offset — time still flows during the offset."""

    def __init__(self, offset_ms: int = 0):
        self.offset_ms = offset_ms

    def __call__(self) -> int:
        return real_clock() + self.offset_ms


class NaiveGenerator:
    """The bug: trusts the clock, wraps the sequence in place."""

    def __init__(self, worker_id: int, clock=real_clock):
        self.worker_id = worker_id
        self.clock = clock
        self.last_ts = -1
        self.seq = 0

    def next_id(self) -> tuple[int, int, int, int]:
        ts = self.clock()
        if ts == self.last_ts:
            self.seq = (self.seq + 1) & MAX_SEQ  # wraps: duplicate after 4096
        else:
            self.last_ts = ts  # accepts ts < last_ts: re-traverses old pairs
            self.seq = 0
        return encode(ts, self.worker_id, self.seq), ts, self.worker_id, self.seq


class Generator:
    """Hardened generator: monotonic output, explicit backwards-clock policy."""

    def __init__(self, worker_id: int, clock=real_clock, policy: str = "error",
                 wait_max_ms: int = WAIT_MAX_MS, sleep=time.sleep):
        assert policy in POLICIES, policy
        self.worker_id = worker_id
        self.clock = clock
        self.policy = policy
        self.wait_max_ms = wait_max_ms
        self.sleep = sleep
        self.last_ts = -1
        self.seq = 0
        self.waited_ms = 0.0

    def _spin_next_ms(self) -> int:
        while True:
            ts = self.clock()
            if ts > self.last_ts:
                return ts

    def next_id(self) -> tuple[int, int, int, int]:
        ts = self.clock()
        if ts < self.last_ts:
            behind = self.last_ts - ts
            if self.policy == "error":
                raise ClockBackwardsError(f"clock is {behind}ms behind last id")
            if self.policy == "wait":
                if behind > self.wait_max_ms:
                    raise ClockBackwardsError(
                        f"clock is {behind}ms behind; wait bound is {self.wait_max_ms}ms"
                    )
                t0 = time.monotonic()
                while ts < self.last_ts:
                    self.sleep(0.001)
                    ts = self.clock()
                self.waited_ms += (time.monotonic() - t0) * 1000
            else:  # hold: issue from last_ts's remaining sequence
                ts = self.last_ts
        if ts == self.last_ts:
            if self.seq >= MAX_SEQ:
                ts = self._spin_next_ms()  # exhausted: refuse to wrap
                self.seq = 0
            else:
                self.seq += 1
        else:
            self.seq = 0
        self.last_ts = ts
        return encode(ts, self.worker_id, self.seq), ts, self.worker_id, self.seq


def selftest() -> int:
    broken = os.environ.get("SELFTEST_BREAK") == "1"
    enc = encode
    if broken:
        # Mis-compose the layout: worker shifted onto the sequence bits.
        def enc(ts_ms, worker, seq):  # noqa: ANN001
            return (ts_ms << (WORKER_BITS + SEQ_BITS)) | (worker << 2) | seq

    failures = []

    # 1. encode/decode round-trip at the boundaries.
    for ts, w, s in [(0, 0, 0), (MAX_TS, MAX_WORKER, MAX_SEQ), (1, 1023, 1),
                     (123_456_789, 511, 2048)]:
        got = decode(enc(ts, w, s))
        if got != (ts, w, s):
            failures.append(f"round-trip {(ts, w, s)} -> {got}")

    # 2. Fits in a signed 64-bit int.
    if enc(MAX_TS, MAX_WORKER, MAX_SEQ) >= 1 << 63:
        failures.append("id overflows a signed BIGINT")

    # 3. k-ordering: increasing clock => strictly increasing ids.
    clock = ScriptedClock([10, 10, 10, 11, 12, 12, 50])
    gen = Generator(worker_id=7, clock=clock, policy="error")
    ids = [gen.next_id()[0] for _ in range(7)]
    if ids != sorted(ids) or len(set(ids)) != len(ids):
        failures.append("output not strictly increasing under increasing clock")

    # 4. Same ms, different workers => different ids that sort by worker.
    a = encode(100, 1, 0)
    b = encode(100, 2, 0)
    if not (a < b and decode(a)[1] == 1 and decode(b)[1] == 2):
        failures.append("worker bits do not separate ids within one ms")

    # 5. The error policy refuses a backwards clock.
    gen = Generator(worker_id=1, clock=ScriptedClock([100, 50]), policy="error")
    gen.next_id()
    try:
        gen.next_id()
        failures.append("error policy accepted a backwards clock")
    except ClockBackwardsError:
        pass

    for f in failures:
        common.log.error("selftest: %s", f)
    if broken:
        # Non-vacuous proof: the broken layout MUST be caught.
        ok = bool(failures)
        common.log.info("selftest(BREAK): %s", "caught the broken layout" if ok
                        else "FAILED TO CATCH a broken layout")
        return 0 if ok else 1
    common.log.info("selftest: %s", "all checks passed" if not failures else "FAILED")
    return 0 if not failures else 1


if __name__ == "__main__":
    sys.exit(selftest())
