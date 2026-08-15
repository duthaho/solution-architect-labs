"""Flip the router mode: phase.py <single|double-write|shadow-read|sharded>.

The write is atomic (temp file + rename), so the running traffic generator
sees either the old mode or the new one, never a torn file — this file IS the
cutover mechanism, the whole reshard flips on one rename() syscall.

The ladder order (single -> double-write -> shadow-read -> sharded) is
LOAD-BEARING, but this script only warns on violations instead of refusing:
failure drill 4 (flip to sharded while the backfill is incomplete) exists
precisely to let you watch what happens when an operator skips a rung.
"""
import sys

from common import MODES, SHADOW_DIFFS, log, read_mode, write_mode


def main() -> None:
    if len(sys.argv) != 2 or sys.argv[1] not in MODES:
        raise SystemExit(f"usage: phase.py <{'|'.join(MODES)}>")
    new = sys.argv[1]
    old = read_mode()
    if new == old:
        log.info("router already in mode %s — nothing to do", new)
        return

    old_i, new_i = MODES.index(old), MODES.index(new)
    if new_i - old_i > 1:
        log.warning("SKIPPING RUNGS: %s -> %s jumps over %s — the ladder order is "
                    "load-bearing (drill 4 shows why)", old, new, MODES[old_i + 1:new_i])
    elif new_i < old_i and not (old == "sharded" and new == "single"):
        log.warning("moving DOWN the ladder %s -> %s (rollback should go through "
                    "rollback.py, which drains the repair queue first)", old, new)

    if new == "shadow-read":
        SHADOW_DIFFS.touch()  # exists-but-empty = "rehearsed and clean", not "never ran"

    write_mode(new)
    log.info("phase: %s -> %s", old, new)


if __name__ == "__main__":
    main()
