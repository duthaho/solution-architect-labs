"""Roll back from `sharded` to `single` — mono becomes authoritative again.

This is only cheap because of a decision made BEFORE cutover: in `sharded`
mode the router keeps mirroring every write back onto mono (the symmetric
twin of double-write). Mono never stopped being current, so rollback is a
queue drain plus one atomic rename — not a reverse migration. The mirroring
costs a dual write per op during the soak; the rollback window it buys is
the whole reason the soak exists. Stop mirroring (the eventual "cleanup"
step) and this script stops working — rollback has an expiry date, and
choosing when to let it expire is the last decision of the reshard.

Order of operations matters — reconcile BEFORE the flip, not after:
  1. drain repair_queue entries targeting mono (mirror legs that failed),
  2. reconcile counts+checksums mono vs shards WHILE STILL SHARDED. Only now
     are both sides being written for every op, so a live-traffic snapshot
     converges on retry. The moment the router flips, mono runs ahead of the
     shards by design and this comparison stops meaning anything — an
     after-the-flip reconciliation would page you forever about drift that
     is supposed to be there.
  3. flip the router to `single` (atomic — traffic follows within an op),
  4. drain once more: a mirror write may have failed between 2 and 3; after
     the flip no NEW mirror entries can appear, so this drain is final.

After this, the shards go stale and stay stale. Truncate them and delete
backfill_state.json before attempting the ladder again.
"""
import sys

from common import SHARDS, connect, log, read_mode, replay_repair_queue, write_mode
from verify import check_misplacement, check_partitions


def main() -> None:
    mode = read_mode()
    if mode != "sharded":
        log.warning("mode is '%s', not 'sharded' — rollback is a no-op ladder-wise; flipping anyway", mode)

    log.info("STEP 1: drain repair queue -> mono (mirror legs that failed during the soak)")
    replay_repair_queue(targets={"mono"})

    log.info("STEP 2: reconcile mono vs shards BEFORE the flip (both sides still written)")
    mono = connect("mono")
    shard_conns = {s: connect(s) for s in SHARDS}
    failures = check_partitions(mono, shard_conns) + check_misplacement(shard_conns)
    mono.close()
    for c in shard_conns.values():
        c.close()
    if failures:
        log.error("❌ ROLLBACK ABORTED: %d reconciliation failures — mono is NOT a faithful "
                  "copy; flipping to it now would make the divergence authoritative. "
                  "Drain the repair queue / investigate, then rerun.", failures)
        sys.exit(1)

    log.info("STEP 3: flip router to single (atomic rename — this IS the rollback)")
    write_mode("single")

    log.info("STEP 4: final drain (mirror legs that raced in between step 2 and the flip)")
    replay_repair_queue(targets={"mono"})

    log.info("✅ ROLLBACK COMPLETE: mono authoritative, traffic never stopped. The shards "
             "are now going stale by design — truncate them (and rm backfill_state.json) "
             "before climbing the ladder again")


if __name__ == "__main__":
    main()
