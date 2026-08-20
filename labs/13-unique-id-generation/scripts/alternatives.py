"""The comparison targets: UUIDv4, UUIDv7, MySQL AUTO_INCREMENT, Redis INCR.

Each scheme exposes a maker returning ``(id_as_int, canonical_text)`` so the
bench can measure them all with one loop. UUIDv7 is implemented per RFC 9562
(48-bit unix ms | ver | 12 rand | var | 62 rand) — time-ordered like a
Snowflake id, but coordination-free and 128 bits wide.
"""

import os
import time
import uuid

RAND_A_MASK = (1 << 12) - 1
RAND_B_MASK = (1 << 62) - 1


def uuid7() -> uuid.UUID:
    """RFC 9562 UUIDv7: big-endian unix ms, then version/variant/randomness."""
    ms = (time.time_ns() // 1_000_000) & ((1 << 48) - 1)
    rand_a = int.from_bytes(os.urandom(2), "big") & RAND_A_MASK
    rand_b = int.from_bytes(os.urandom(8), "big") & RAND_B_MASK
    value = (ms << 80) | (0x7 << 76) | (rand_a << 64) | (0b10 << 62) | rand_b
    return uuid.UUID(int=value)


def make_uuid4() -> tuple[int, str]:
    u = uuid.uuid4()
    return u.int, str(u)


def make_uuid7() -> tuple[int, str]:
    u = uuid7()
    return u.int, str(u)


class AutoincMaker:
    """One row round-trip per id: MySQL hands out the sequence."""

    def __init__(self, conn):
        self.conn = conn

    def __call__(self) -> tuple[int, str]:
        with self.conn.cursor() as cur:
            cur.execute("INSERT INTO seq_autoinc (payload) VALUES (0)")
            id_ = cur.lastrowid
        self.conn.commit()
        return id_, str(id_)


class RedisIncrMaker:
    """One INCR round-trip per id: a central counter, no gaps, no sort keys."""

    def __init__(self, client, key: str = "lab13:seq"):
        self.client = client
        self.key = key

    def __call__(self) -> tuple[int, str]:
        id_ = self.client.incr(self.key)
        return id_, str(id_)
