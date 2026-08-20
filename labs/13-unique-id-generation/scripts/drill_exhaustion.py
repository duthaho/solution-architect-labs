"""The sequence-exhaustion drill: more than 4096 draws inside one millisecond.

The 12-bit sequence caps a worker at 4096 ids/ms (~4M ids/s). A scripted
clock stays frozen at one logical ms for 5000 draws, then auto-advances —
so the hardened generator's spin-to-next-ms provably terminates.

  naive     wraps the sequence in place: draw 4097 re-issues (ts, seq=0) —
            904 duplicates out of 5000, silently
  hardened  refuses to wrap: at seq 4095 it spins until the clock moves,
            then continues at (ts+1, seq=0) — 5000 ids, all unique

Journals: ids_exhaustion_naive.jsonl / ids_exhaustion_hardened.jsonl.
Exit 0 iff naive duplicated exactly DRAWS-4096 ids AND hardened emitted
DRAWS unique, strictly monotonic ids.
"""

import sys

import common
import snowflake

DRAWS = 5000
FROZEN_TS = 500
EXPECTED_DUPES = DRAWS - (snowflake.MAX_SEQ + 1)  # 904


def frozen_then_advance() -> snowflake.ScriptedClock:
    """Frozen at FROZEN_TS for DRAWS reads, then the next ms forever."""
    return snowflake.ScriptedClock([FROZEN_TS] * DRAWS,
                                   then=lambda: FROZEN_TS + 1)


def run(kind: str) -> tuple[int, int, bool]:
    journal = common.ids_journal_path(f"exhaustion_{kind}")
    journal.unlink(missing_ok=True)
    if kind == "naive":
        gen = snowflake.NaiveGenerator(5, clock=frozen_then_advance())
    else:
        gen = snowflake.Generator(6, clock=frozen_then_advance(), policy="error")
    ids = []
    for _ in range(DRAWS):
        id_, ts, w, seq = gen.next_id()
        ids.append(id_)
        common.append_jsonl(journal, {"id": id_, "ts": ts, "worker": w, "seq": seq})
    return len(ids), len(ids) - len(set(ids)), ids == sorted(ids)


def main() -> int:
    n_naive, d_naive, _ = run("naive")
    n_hard, d_hard, monotonic = run("hardened")

    print(f"\n{'generator':10} {'ids':>6} {'dupes':>6} {'monotonic':>10}")
    print(f"{'naive':10} {n_naive:>6} {d_naive:>6} {'-':>10}")
    print(f"{'hardened':10} {n_hard:>6} {d_hard:>6} {str(monotonic):>10}\n")

    if d_naive != EXPECTED_DUPES:
        common.log.error("naive: expected exactly %d duplicates, got %d",
                         EXPECTED_DUPES, d_naive)
        return 1
    if not (n_hard == DRAWS and d_hard == 0 and monotonic):
        common.log.error("hardened: ids=%d dupes=%d monotonic=%s",
                         n_hard, d_hard, monotonic)
        return 1
    common.log.info(
        "exhaustion: naive wrapped (%d dupes), hardened spun to the next ms — exit 0",
        d_naive,
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
