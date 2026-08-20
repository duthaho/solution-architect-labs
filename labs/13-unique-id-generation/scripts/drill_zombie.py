"""The zombie-worker drill: a stalled generator's worker id is reclaimed.

The scenario (the reason worker-id reuse is scary at all):

1. Worker A claims a worker id and emits IDs, heartbeating its lease.
2. A stalls — SIGSTOP, standing in for a GC pause / VM freeze — long past
   its lease TTL. It cannot heartbeat while stopped.
3. Worker B claims the now-expired row and gets **the same worker id**.
4. A wakes up (SIGCONT) believing nothing happened.

Both processes draw timestamps from the same file-scripted logical clock
(`clock.txt`): the orchestrator freezes phase 1 at ts=1000 and phase 2 at
ts=2000, so a resumed zombie that keeps emitting traverses **exactly** the
(ts=2000, seq=0..) pairs B is emitting — duplicates are guaranteed by
construction, not left to timing luck.

Exit-code contract:
  default        -> exit 0 iff the worker id WAS reused AND A emitted 0 IDs
                    after resuming (the validity window refused) AND zero
                    duplicate IDs across both journals
  ZOMBIE_NAIVE=1 -> exit 0 iff the duplicate WAS reproduced (A, with the
                    lease check disabled, collides with B on identical
                    (ts, worker, seq) triples)

Run after `make bootstrap`; journals: ids_zombie[_naive]_{a,b}.jsonl.
"""

import os
import signal
import subprocess
import sys
import time

import common
import lease as lease_mod
import snowflake

NAIVE = os.environ.get("ZOMBIE_NAIVE") == "1"
ROLE = os.environ.get("ZOMBIE_ROLE", "")
TAG = "zombie_naive" if NAIVE else "zombie"

TTL_MS = 1500
MARGIN_MS = 300
PHASE1_TS = 1000
PHASE2_TS = 2000
A_MIN_PHASE1 = 20      # orchestrator stops A after it has journaled this many
A_POST_RESUME_CAP = 50  # naive A emits this many after resuming, then exits
B_COUNT = 100


def file_clock() -> int:
    return int(common.CLOCK_PATH.read_text().strip())


def journal(role: str) -> "common.Path":
    return common.ids_journal_path(f"{TAG}_{role}")


def run_a() -> int:
    """Claim, emit under the lease, get stopped, resume, behave per mode."""
    conn = common.connect(autocommit=True)
    lse = lease_mod.Lease(ttl_ms=TTL_MS, margin_ms=MARGIN_MS, owner="A-zombie")
    wid = lse.claim(conn)
    if wid is None:
        return 2
    gen = lease_mod.LeasedGenerator(lse, clock=file_clock, policy="hold",
                                    check_lease=not NAIVE)
    emitted_p2 = 0
    n = 0
    while True:
        try:
            id_, ts, w, seq = gen.next_id()
        except lease_mod.LeaseLostError:
            common.log.info("A: validity window closed — refusing to emit, exiting")
            return 0
        common.append_jsonl(journal("a"), {
            "id": id_, "ts": ts, "worker": w, "seq": seq, "role": "a",
            "phase": 2 if ts >= PHASE2_TS else 1,
        })
        n += 1
        if ts >= PHASE2_TS:
            emitted_p2 += 1
            if emitted_p2 >= A_POST_RESUME_CAP:
                common.log.info("A: naive zombie emitted %d post-resume ids", emitted_p2)
                return 0
        if n % 10 == 0 and emitted_p2 == 0:
            try:
                lse.heartbeat(conn)
            except lease_mod.LeaseLostError:
                if not NAIVE:
                    common.log.info("A: heartbeat says lease lost — exiting")
                    return 0
                # the zombie ignores even a failed heartbeat
        time.sleep(0.01)


def run_b() -> int:
    """Claim the expired id, emit B_COUNT ids in phase 2, release, exit."""
    conn = common.connect(autocommit=True)
    lse = lease_mod.Lease(ttl_ms=10_000, margin_ms=300, owner="B-fresh")
    wid = lse.claim(conn)
    if wid is None:
        return 2
    common.log.info("B: claimed worker id %d", wid)
    gen = lease_mod.LeasedGenerator(lse, clock=file_clock, policy="hold")
    for _ in range(B_COUNT):
        id_, ts, w, seq = gen.next_id()
        common.append_jsonl(journal("b"), {
            "id": id_, "ts": ts, "worker": w, "seq": seq, "role": "b", "phase": 2,
        })
    lse.release(conn)
    return 0


def wait_journal(path, count: int, timeout_s: float) -> bool:
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        if len(common.read_jsonl(path)) >= count:
            return True
        time.sleep(0.05)
    return False


def orchestrate() -> int:
    for role in ("a", "b"):
        journal(role).unlink(missing_ok=True)
    common.CLOCK_PATH.write_text(str(PHASE1_TS))

    env = dict(os.environ)
    a = subprocess.Popen([sys.executable, __file__],
                         env={**env, "ZOMBIE_ROLE": "A"})
    if not wait_journal(journal("a"), A_MIN_PHASE1, 30):
        a.kill()
        common.log.error("A never produced phase-1 ids")
        return 1

    os.kill(a.pid, signal.SIGSTOP)
    common.log.info("A stopped (SIGSTOP) after %d ids; letting the lease expire",
                    len(common.read_jsonl(journal("a"))))
    time.sleep((TTL_MS + 400) / 1000)

    common.CLOCK_PATH.write_text(str(PHASE2_TS))
    rb = subprocess.run([sys.executable, __file__],
                        env={**env, "ZOMBIE_ROLE": "B"}).returncode
    if rb != 0:
        os.kill(a.pid, signal.SIGCONT)
        a.kill()
        common.log.error("B failed (%d)", rb)
        return 1

    common.log.info("B done; resuming the zombie (SIGCONT)")
    os.kill(a.pid, signal.SIGCONT)
    ra = a.wait(timeout=30)
    if ra != 0:
        common.log.error("A exited %d", ra)
        return 1

    ids_a = common.read_jsonl(journal("a"))
    ids_b = common.read_jsonl(journal("b"))
    reused = bool(ids_b) and ids_b[0]["worker"] == ids_a[0]["worker"]
    post_resume = sum(1 for r in ids_a if r["phase"] == 2)
    dupes = len([i for i in (r["id"] for r in ids_a)
                 if i in {r["id"] for r in ids_b}])

    common.log.info(
        "worker id reused=%s | A ids=%d (post-resume=%d) B ids=%d | duplicates=%d",
        reused, len(ids_a), post_resume, len(ids_b), dupes,
    )

    if not reused:
        common.log.error("B did not reuse A's worker id — drill setup broken")
        return 1
    if NAIVE:
        if dupes > 0 and post_resume > 0:
            common.log.info("naive zombie collision reproduced (expected) — exit 0")
            return 0
        common.log.error("naive mode failed to reproduce the collision")
        return 1
    if post_resume == 0 and dupes == 0:
        common.log.info("hardened zombie refused post-resume; zero duplicates — exit 0")
        return 0
    common.log.error("hardened mode leaked: post_resume=%d dupes=%d", post_resume, dupes)
    return 1


def main() -> int:
    if ROLE == "A":
        return run_a()
    if ROLE == "B":
        return run_b()
    return orchestrate()


if __name__ == "__main__":
    sys.exit(main())
