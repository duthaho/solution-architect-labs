"""Worker-ID leases in MySQL: claim, heartbeat, and the local validity window.

The contract that makes ID reuse safe:

- A generator may emit only while ``now < safe_until`` where
  ``safe_until = lease.expires_at - LEASE_MARGIN_MS`` (all wall-clock ms).
  The margin is the safety gap: the DB considers a lease reclaimable at
  ``expires_at``, but the holder stops emitting strictly earlier — so a
  stalled holder is guaranteed dead-to-the-ID before anyone can take it.
- Validity is checked **locally** (one comparison per ID, no DB round-trip);
  only heartbeats touch the DB, and a heartbeat that matched 0 rows means
  the lease is lost, never "retry".

Every claim / renew / lost / release event is appended to
``lease_events.jsonl`` — the verifier's evidence for "no two owners held the
same worker id at once".

Run this file directly for the inline demo: two claimants get different ids,
an expired lease is reclaimable by a third, exit 0.
"""

import os
import sys
import time
import uuid

import common
import snowflake


class LeaseLostError(RuntimeError):
    """The local validity window closed (or a heartbeat matched 0 rows)."""


class Lease:
    def __init__(self, ttl_ms: int | None = None, margin_ms: int | None = None,
                 owner: str | None = None):
        self.ttl_ms = ttl_ms if ttl_ms is not None else common.LEASE_TTL_MS
        self.margin_ms = margin_ms if margin_ms is not None else common.LEASE_MARGIN_MS
        assert self.margin_ms < self.ttl_ms
        self.owner = owner or f"{os.getpid()}-{uuid.uuid4().hex[:8]}"
        self.worker_id: int | None = None
        self.expires_at = 0  # unix ms, as written to the DB

    @property
    def safe_until(self) -> int:
        return self.expires_at - self.margin_ms

    def valid(self) -> bool:
        return self.worker_id is not None and common.now_ms() < self.safe_until

    def _journal(self, event: str) -> None:
        common.append_jsonl(common.LEASE_EVENTS_PATH, {
            "event": event, "worker_id": self.worker_id, "owner": self.owner,
            "expires_at": self.expires_at, "safe_until": self.safe_until,
            "at": common.now_ms(),
        })

    def claim(self, conn) -> int | None:
        """Claim the lowest free-or-expired worker id. One transaction.

        Lease connections must be autocommit: heartbeat/release are then
        single statements committed server-side, so a client frozen
        mid-operation (SIGSTOP, GC pause) can never keep holding a row lock
        that blocks other claimants. The claim itself opens an explicit
        transaction around its SELECT ... FOR UPDATE + UPDATE pair.
        """
        assert conn.get_autocommit(), "lease operations require an autocommit connection"
        now = common.now_ms()
        conn.begin()
        with conn.cursor() as cur:
            cur.execute(
                "SELECT worker_id FROM worker_leases "
                "WHERE owner IS NULL OR expires_at < %s "
                "ORDER BY worker_id LIMIT 1 FOR UPDATE",
                (now,),
            )
            row = cur.fetchone()
            if row is None:
                conn.rollback()
                return None
            self.worker_id = row[0]
            self.expires_at = now + self.ttl_ms
            cur.execute(
                "UPDATE worker_leases SET owner = %s, expires_at = %s "
                "WHERE worker_id = %s",
                (self.owner, self.expires_at, self.worker_id),
            )
        conn.commit()
        self._journal("claim")
        return self.worker_id

    def heartbeat(self, conn) -> None:
        """Renew, or learn the lease is lost. 0 matched rows is never a retry.

        Single autocommitted statement (see claim); rowcount means *matched*
        because connections set CLIENT.FOUND_ROWS.
        """
        now = common.now_ms()
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE worker_leases SET expires_at = %s "
                "WHERE worker_id = %s AND owner = %s",
                (now + self.ttl_ms, self.worker_id, self.owner),
            )
            matched = cur.rowcount
        if matched != 1:
            self._journal("lost")
            raise LeaseLostError(
                f"worker id {self.worker_id} no longer owned by {self.owner}"
            )
        self.expires_at = now + self.ttl_ms
        self._journal("renew")

    def release(self, conn) -> None:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE worker_leases SET owner = NULL, expires_at = 0 "
                "WHERE worker_id = %s AND owner = %s",
                (self.worker_id, self.owner),
            )
        self._journal("release")
        self.worker_id = None
        self.expires_at = 0


class LeasedGenerator:
    """A hardened generator that refuses to emit outside its lease window.

    ``check_lease=False`` is the deliberately-broken zombie mode: the
    generator keeps emitting after the window closes — the drill uses it to
    prove what the check is protecting against.
    """

    def __init__(self, lease: Lease, clock=snowflake.real_clock,
                 policy: str = "error", check_lease: bool = True):
        assert lease.worker_id is not None, "claim the lease first"
        self.lease = lease
        self.check_lease = check_lease
        self.gen = snowflake.Generator(lease.worker_id, clock=clock, policy=policy)

    def next_id(self) -> tuple[int, int, int, int]:
        if self.check_lease and not self.lease.valid():
            raise LeaseLostError(
                f"validity window closed for worker id {self.lease.worker_id}"
            )
        return self.gen.next_id()


def demo() -> int:
    """Two claimants get different ids; an expired lease is reclaimable."""
    ttl, margin = 1200, 300
    conn = common.connect(autocommit=True)

    a, b = Lease(ttl, margin), Lease(ttl, margin)
    wa, wb = a.claim(conn), b.claim(conn)
    common.log.info("A claimed %s, B claimed %s", wa, wb)
    if wa is None or wb is None or wa == wb:
        common.log.error("claims not distinct")
        return 1

    gen = LeasedGenerator(a)
    id0 = gen.next_id()[0]
    common.log.info("A emitted id %d while valid", id0)

    b.heartbeat(conn)  # B renews; A goes silent and expires
    time.sleep((ttl + 100) / 1000)

    c = Lease(ttl, margin)
    wc = c.claim(conn)
    common.log.info("C claimed %s after A expired (B renewed, keeps %s)", wc, wb)
    if wc != wa:
        common.log.error("C should reclaim A's expired id %s, got %s", wa, wc)
        return 1

    try:
        gen.next_id()
        common.log.error("A emitted after its window closed")
        return 1
    except LeaseLostError:
        common.log.info("A's generator refused to emit past safe_until")

    try:
        a.heartbeat(conn)
        common.log.error("A's heartbeat should have failed")
        return 1
    except LeaseLostError:
        common.log.info("A's heartbeat reported the lease as lost")

    for lease in (b, c):
        lease.release(conn)
    conn.close()
    common.log.info("lease demo: all checks passed")
    return 0


if __name__ == "__main__":
    sys.exit(demo())
