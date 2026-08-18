"""Drill C2 — one giant DELETE vs batched deletes, measured, not asserted by vibes.

Two phases of equal magnitude on lab10_c.order_items:
  giant:    flag 10% of rows (id%10=3), delete them in ONE statement/transaction
  batched:  flag another 10% (id%10=4), delete LIMIT-batch at a time, short
            transactions with pauses

Throughout both phases a reader thread does a point SELECT ... FOR SHARE on a
random row FROM THE SET BEING DELETED, every ~5ms. That is the honest probe:
it queues behind the deleter's X locks exactly like any locking read path
(FK checks, UPSERTs, SELECT-then-write) that touches a tombstoned row would.
During the giant statement every such probe waits for the FULL remaining
transaction; during the batched phase it waits for at most one small batch.

Objective pass criterion: reader p95 during the batched phase must be lower
than during the giant phase. Non-zero exit otherwise.
"""
import random
import threading
import time

from common import connect, log, percentiles

LIVE = "lab10_c"
BATCH = 1000


class Reader(threading.Thread):
    def __init__(self):
        super().__init__(daemon=True)
        self.samples: list[tuple[str, float]] = []
        self.phase = "idle"
        self.targets: list[int] = []
        self.stop = False

    def run(self):
        conn = connect()
        rng = random.Random(3)
        with conn.cursor() as cur:
            cur.execute("SET SESSION innodb_lock_wait_timeout = 120")
            while not self.stop:
                if self.phase == "idle" or not self.targets:
                    time.sleep(0.005)
                    continue
                rid = rng.choice(self.targets)
                phase = self.phase
                t0 = time.perf_counter()
                cur.execute(f"SELECT id, qty FROM {LIVE}.order_items "
                            f"WHERE id=%s FOR SHARE", (rid,))
                cur.fetchall()
                self.samples.append((phase, (time.perf_counter() - t0) * 1000))
                time.sleep(0.005)
        conn.close()

    def p95(self, phase: str) -> float:
        lat = [ms for p, ms in self.samples if p == phase]
        return percentiles(lat)[1]


def pick_work(conn) -> tuple[list[int], list[int]]:
    """Two equal-size disjoint sets of live item ids (~10% of the table each)."""
    with conn.cursor() as cur:
        cur.execute(f"SELECT COUNT(*) FROM {LIVE}.order_items WHERE deleted_at IS NULL")
        live = cur.fetchone()[0]
        n = max(live // 10, 500)
        cur.execute(f"SELECT id FROM {LIVE}.order_items WHERE deleted_at IS NULL "
                    f"ORDER BY id LIMIT %s", (2 * n,))
        ids = [r[0] for r in cur.fetchall()]
    conn.commit()  # end the read snapshot
    assert len(ids) == 2 * n, "not enough live order_items — re-run make seed"
    return ids[:n], ids[n:]


def flag(conn, ids: list[int]) -> None:
    with conn.cursor() as cur:
        cur.execute(f"UPDATE {LIVE}.order_items SET deleted_at=NOW(6) "
                    f"WHERE id IN ({','.join(map(str, ids))})")
    conn.commit()


def main() -> None:
    conn = connect(autocommit=False)
    reader = Reader()
    reader.start()

    ids1, ids2 = pick_work(conn)
    flag(conn, ids1)
    n1 = len(ids1)
    reader.targets = ids1
    reader.phase = "giant"
    t0 = time.perf_counter()
    with conn.cursor() as cur:
        cur.execute(f"DELETE FROM {LIVE}.order_items WHERE deleted_at IS NOT NULL")
        deleted1 = cur.rowcount
    conn.commit()
    giant_s = time.perf_counter() - t0
    reader.phase = "idle"
    log.info("giant:   %d rows in ONE transaction, %.2fs — locks held the whole time",
             deleted1, giant_s)
    time.sleep(0.5)

    flag(conn, ids2)
    n2 = len(ids2)
    reader.targets = ids2
    reader.phase = "batched"
    t0 = time.perf_counter()
    deleted2 = 0
    while True:
        with conn.cursor() as cur:
            cur.execute(f"DELETE FROM {LIVE}.order_items "
                        f"WHERE deleted_at IS NOT NULL LIMIT {BATCH}")
            n = cur.rowcount
        conn.commit()
        deleted2 += n
        if n < BATCH:
            break
        time.sleep(0.05)
    batched_s = time.perf_counter() - t0
    reader.phase = "idle"
    log.info("batched: %d rows in %d-row transactions, %.2fs wall (incl. pauses)",
             deleted2, BATCH, batched_s)

    reader.stop = True
    reader.join()
    g95, b95 = reader.p95("giant"), reader.p95("batched")
    log.info("concurrent reader p95: giant=%.1fms  batched=%.1fms  (flagged %d vs %d)",
             g95, b95, n1, n2)
    assert b95 < g95, (
        f"expected batched deletes to hurt readers less (batched p95={b95:.1f}ms, "
        f"giant p95={g95:.1f}ms) — rerun with a larger SEED_ROWS")
    log.info("drill C2 PASSED — the slow path is FASTER for everyone else: "
             "batches trade wall-clock time for lock-time")
    conn.close()


if __name__ == "__main__":
    main()
