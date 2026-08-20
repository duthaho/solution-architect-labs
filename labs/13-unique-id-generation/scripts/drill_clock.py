"""The backwards-clock drill: four policies against the same regression.

Scripted scenario (logical ms): emit through ts 100 → 101 → 102, then the
clock jumps back to 100 — an NTP step, a VM migration, a leap smear gone
wrong. What happens next is pure policy:

  naive  re-traverses (100, seq=0..2) — three duplicates, silently
  error  raises ClockBackwardsError on the first regressed draw
  wait   (real-time + offset clock) sleeps the regression out, then continues
  hold   keeps issuing at last_ts=102 from the remaining sequence — no wait,
         no dupes — and spills forward when the clock recovers

Every policy journals to ids_clock_<policy>.jsonl (worker ids 1..4 keep the
streams disjoint). Exit 0 iff naive produced exactly its 3 expected
duplicates AND error raised AND wait/hold produced their expected counts
with zero duplicates.
"""

import sys

import common
import snowflake

GOOD = [100, 100, 100, 101, 101, 101, 102, 102, 102]  # 9 clean draws
JUMP_BACK = [100, 100, 100]
WAIT_JUMP_MS = 200
WAIT_BATCH = 30


def emit(gen, journal_name: str, n: int, results: list) -> int:
    """Draw n ids, journal them; return count actually emitted (errors stop)."""
    emitted = 0
    for _ in range(n):
        try:
            id_, ts, w, seq = gen.next_id()
        except snowflake.ClockBackwardsError as e:
            results.append(("error_raised", str(e)))
            break
        common.append_jsonl(common.ids_journal_path(journal_name), {
            "id": id_, "ts": ts, "worker": w, "seq": seq,
        })
        emitted += 1
    return emitted


def dupes_in(journal_name: str) -> int:
    ids = [r["id"] for r in common.read_jsonl(common.ids_journal_path(journal_name))]
    return len(ids) - len(set(ids))


def main() -> int:
    rows = []
    ok = True

    # --- naive: the bug ---------------------------------------------------
    common.ids_journal_path("clock_naive").unlink(missing_ok=True)
    gen = snowflake.NaiveGenerator(1, clock=snowflake.ScriptedClock(GOOD + JUMP_BACK))
    n = emit(gen, "clock_naive", len(GOOD) + len(JUMP_BACK), [])
    d = dupes_in("clock_naive")
    rows.append(("naive", n, d, 0, 0.0))
    if d != len(JUMP_BACK):
        common.log.error("naive: expected exactly %d duplicates, got %d",
                         len(JUMP_BACK), d)
        ok = False

    # --- error: refuse ----------------------------------------------------
    common.ids_journal_path("clock_error").unlink(missing_ok=True)
    events: list = []
    gen = snowflake.Generator(2, clock=snowflake.ScriptedClock(GOOD + JUMP_BACK),
                              policy="error")
    n = emit(gen, "clock_error", len(GOOD) + len(JUMP_BACK), events)
    d = dupes_in("clock_error")
    rows.append(("error", n, d, len(events), 0.0))
    if not (n == len(GOOD) and len(events) == 1 and d == 0):
        common.log.error("error policy: emitted=%d raised=%d dupes=%d", n,
                         len(events), d)
        ok = False

    # --- wait: sleep it out (real time flows under the offset) -----------
    common.ids_journal_path("clock_wait").unlink(missing_ok=True)
    clock = snowflake.OffsetClock()
    gen = snowflake.Generator(3, clock=clock, policy="wait")
    n = emit(gen, "clock_wait", WAIT_BATCH, [])
    clock.offset_ms -= WAIT_JUMP_MS  # the backwards step
    n += emit(gen, "clock_wait", WAIT_BATCH, [])
    d = dupes_in("clock_wait")
    rows.append(("wait", n, d, 0, round(gen.waited_ms, 1)))
    if not (n == 2 * WAIT_BATCH and d == 0 and gen.waited_ms >= WAIT_JUMP_MS * 0.75):
        common.log.error("wait policy: emitted=%d dupes=%d waited=%.1fms", n, d,
                         gen.waited_ms)
        ok = False

    # --- hold: issue from the remaining sequence at last_ts ---------------
    common.ids_journal_path("clock_hold").unlink(missing_ok=True)
    script = GOOD + [100] * 5 + [103, 103]  # regressed draws + recovery
    gen = snowflake.Generator(4, clock=snowflake.ScriptedClock(script), policy="hold")
    n = emit(gen, "clock_hold", len(script), [])
    d = dupes_in("clock_hold")
    ids = [r["id"] for r in common.read_jsonl(common.ids_journal_path("clock_hold"))]
    monotonic = ids == sorted(ids)
    rows.append(("hold", n, d, 0, 0.0))
    if not (n == len(script) and d == 0 and monotonic):
        common.log.error("hold policy: emitted=%d dupes=%d monotonic=%s", n, d,
                         monotonic)
        ok = False

    print(f"\n{'policy':8} {'ids':>5} {'dupes':>6} {'errors':>7} {'waited_ms':>10}")
    for name, n, d, e, w in rows:
        print(f"{name:8} {n:>5} {d:>6} {e:>7} {w:>10}")
    print()

    if ok:
        common.log.info("clock drill: naive duplicated, every policy held — exit 0")
        return 0
    common.log.error("clock drill FAILED")
    return 1


if __name__ == "__main__":
    sys.exit(main())
