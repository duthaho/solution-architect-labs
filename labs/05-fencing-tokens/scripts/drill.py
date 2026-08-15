"""Drill orchestrator. Every scenario is event-gated (no sleep-and-hope):

  happy       both workers, no chaos             -> clean ledger, false confidence
  corrupt     SIGSTOP holder past TTL, no fence  -> interleaved ledger
  check-race  holder re-checks before each write -> STILL corrupts (TOCTOU)
  fenced      same chaos as corrupt, fencing on  -> 409s, clean ledger
  compare     side-by-side table of the last corrupt vs fenced runs
"""
import json
import sys
import time

import chaos
import verify
from common import (LAB_DIR, LOCK_KEY, FENCE_KEY, RESULTS, WORKERS,
                    compose, docker, log, record_result, redis_client,
                    worker_logs)


def banner(title: str) -> None:
    print(f"\n{'=' * 74}\n  DRILL: {title}\n{'=' * 74}")


def setup(fencing: str) -> None:
    """Fresh storage (empty ledger, right mode), fresh lock state, no workers."""
    for w in WORKERS.values():
        docker("rm", "-f", w["container"], check=False)
    compose("up", "-d", "--wait", "redis")
    compose("up", "-d", "--wait", "--force-recreate", "storage",
            env={"FENCING": fencing})
    r = redis_client()
    r.delete(LOCK_KEY, FENCE_KEY)
    log.info("setup: fencing=%s, ledger empty, lock state cleared", fencing)


def start_worker(worker: str, *args: str) -> None:
    compose("run", "-d", "--no-deps", "--name", WORKERS[worker]["container"],
            "worker", "python", "scripts/worker.py", "--id", worker, *args)
    log.info("started worker %s (%s)", worker, " ".join(args))


def dump_logs() -> None:
    for w in ("A", "B"):
        logs = worker_logs(w)
        if logs:
            print(f"\n  worker {w} said (its own beliefs, timestamped):")
            for line in logs.splitlines():
                print(f"    {line}")


def finish(scenario: str, expect: str) -> dict:
    dump_logs()
    verdict = verify.run(expect=expect)
    record_result(scenario=scenario, corrupt=verdict["corrupt"],
                  entries=verdict["n_entries"],
                  rejected=len(verdict["rejected"]), ts=int(time.time()))
    return verdict


# ------------------------------------------------------------------ scenarios

def drill_happy() -> None:
    banner("happy path — no pauses, the lock 'works' (this is how trust is born)")
    setup(fencing="off")
    start_worker("A", "--ttl-ms", "8000", "--appends", "4", "--interval", "0.3")
    chaos.wait_ledger_entries("A", 1)
    start_worker("B", "--ttl-ms", "8000", "--appends", "4", "--interval", "0.3")
    chaos.wait_worker_exit("A")
    chaos.wait_worker_exit("B")
    finish("happy", expect="clean")
    print("\n  Locks look bulletproof when nobody pauses. Now run "
          "`make drill-corrupt`.")


def _pause_past_ttl_then_b(a_args: list[str], b_args: list[str]) -> None:
    """Shared chaos schedule: freeze A mid-section past TTL, let B in, thaw A."""
    start_worker("A", *a_args)
    chaos.wait_ledger_entries("A", 2)          # A is mid-critical-section
    chaos.pause("A")                           # "GC pause" begins
    chaos.wait_lock_expired()                  # TTL fires while A is frozen
    start_worker("B", *b_args)                 # B legitimately acquires
    chaos.wait_ledger_entries("B", 2)          # B is mid-critical-section
    chaos.resume("A")                          # A wakes, believes it holds the lock
    chaos.wait_worker_exit("A")
    chaos.wait_worker_exit("B")


def drill_corrupt() -> None:
    banner("corrupt — SIGSTOP the holder past TTL, fencing OFF")
    setup(fencing="off")
    _pause_past_ttl_then_b(
        a_args=["--ttl-ms", "2000", "--appends", "6", "--interval", "0.4"],
        b_args=["--ttl-ms", "10000", "--appends", "6", "--interval", "0.4"])
    finish("corrupt", expect="corrupt")
    print("\n  A never misbehaved: it acquired correctly, wrote within what it\n"
          "  believed was its lease, released politely. The TTL + pause did the\n"
          "  rest. Note A's RELEASE FAILED line — it found out LAST.")


def drill_check_race() -> None:
    banner("check-race — 'just re-check the lock before writing' is not a fix")
    setup(fencing="off")
    # A re-checks before every write; the forced post-check delay is where a
    # real GC pause would land. Chaos freezes A between CHECK_OK and the write.
    start_worker("A", "--mode", "recheck", "--ttl-ms", "4000",
                 "--appends", "4", "--interval", "0.3",
                 "--post-check-delay", "2.0")
    chaos.wait_log_line("A", "CHECK_OK seq=2")   # check passed...
    chaos.pause("A")                             # ...pause lands before the write
    chaos.wait_lock_expired()
    start_worker("B", "--ttl-ms", "8000", "--appends", "4", "--interval", "0.2")
    chaos.wait_worker_exit("B")                  # B does its whole section, releases
    chaos.resume("A")                            # A completes the already-approved write
    chaos.wait_worker_exit("A")
    finish("check-race", expect="corrupt")
    print("\n  A checked, the check PASSED, and the write still landed inside\n"
          "  B's completed section. Check-then-write is a TOCTOU race: the\n"
          "  pause fits in the gap, and the gap cannot be closed client-side.")


def drill_fenced() -> None:
    banner("fenced — identical chaos, fencing ON: storage refuses the stale token")
    setup(fencing="on")
    _pause_past_ttl_then_b(
        a_args=["--ttl-ms", "2000", "--appends", "6", "--interval", "0.4"],
        b_args=["--ttl-ms", "10000", "--appends", "6", "--interval", "0.4"])
    verdict = finish("fenced", expect="clean")
    assert verdict["rejected"], "expected at least one fenced-off write"
    assert "REJECTED 409" in worker_logs("A"), "A should have logged the 409"
    print("\n  Same pause, same zombie, same stale belief — but A's write\n"
          "  carried token 1 and storage had already seen token 2. One integer\n"
          "  comparison did what the lock never could. A was not fixed; it was\n"
          "  made HARMLESS.")


def drill_compare() -> None:
    banner("corrupt vs fenced — side by side")
    rows = {}
    if RESULTS.exists():
        for line in RESULTS.read_text().splitlines():
            rec = json.loads(line)
            rows[rec["scenario"]] = rec          # keep the latest per scenario
    print(f"\n  {'scenario':<12} {'chaos':<26} {'fencing':<8} "
          f"{'ledger':<12} {'stale writes':<14} verdict")
    print("  " + "-" * 84)
    for name, fencing, chaos_desc in (("corrupt", "off", "SIGSTOP past TTL"),
                                      ("fenced", "on", "SIGSTOP past TTL (same)")):
        r = rows.get(name)
        if not r:
            print(f"  {name:<12} (not run yet)")
            continue
        verdict = "✗ CORRUPTED" if r["corrupt"] else "✓ clean"
        blocked = f"{r['rejected']} rejected 409" if r["rejected"] else "0 (accepted!)"
        print(f"  {name:<12} {chaos_desc:<26} {fencing:<8} "
              f"{str(r['entries']) + ' entries':<12} {blocked:<14} {verdict}")
    print("\n  The only difference between the two runs is WHERE correctness\n"
          "  lives: in the client's belief about a TTL (corrupt), or in the\n"
          "  storage layer's token comparison (fenced).")


def main() -> None:
    scenario = sys.argv[1] if len(sys.argv) > 1 else "corrupt"
    {
        "happy": drill_happy,
        "corrupt": drill_corrupt,
        "check-race": drill_check_race,
        "fenced": drill_fenced,
        "compare": drill_compare,
    }[scenario]()


if __name__ == "__main__":
    main()
