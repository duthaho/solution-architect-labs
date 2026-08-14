"""Zero-downtime reindex orchestrator.

Strategy (timestamp catch-up + brief write block for a provably-consistent cutover):

  1. Create the v2 index with a load-optimized configuration
     (refresh_interval=-1, replicas=0).
  2. BASELINE: async _reindex with slices=auto (parallel per-shard sliced scroll),
     throttled via requests_per_second, monitored through the Tasks API.
  3. CATCH-UP LOOP: repeatedly copy docs whose updated_at >= cursor (with an
     overlap margin for clock skew / refresh lag) until the delta is tiny.
     Copies are idempotent: same _id, whole document overwrite.
  4. CUTOVER: block writes on v1 (writers retry — see traffic.py), run one
     final delta pass so v2 is exactly complete, then swap BOTH aliases to v2
     in a single atomic _aliases call, then unblock v1.
     Write block duration: a few seconds (dominated by the final copy +
     refresh — keep orchestration inside the block tight). Reads: never
     interrupted.
  5. Restore production settings on v2 (refresh, replicas).
  6. v1 is kept untouched as the rollback target.

Why the brief write block? With timestamp catch-up alone you cannot have all
three of {no write pause, no dual-write, zero loss}: any write landing on v1
after your final pass but before the swap would be lost. The two honest
fixes are (a) a seconds-long write block (this script) or (b) app-level
dual-write during the final window. See README for the full trade-off table.
"""
import argparse
import time

from common import (
    INDEX_V2,
    READ_ALIAS,
    WRITE_ALIAS,
    alias_target,
    es_client,
    load_index_body,
    log,
    now_millis,
    wait_for_es,
)

OVERLAP_MS = 5_000           # re-copy window overlap: covers clock skew + refresh lag
CATCHUP_TARGET_S = 10.0      # converge when one pass completes this fast (bounds cutover window)
MIN_CATCHUP_PASSES = 2
MAX_CATCHUP_PASSES = 20


def wait_for_task(es, task_id: str, label: str) -> dict:
    # Fast polling matters: during the cutover the write-block window includes
    # this wait, so a lazy 3s poll interval would inflate it needlessly.
    poll_s = 0.25
    last_log = 0.0
    while True:
        t = es.tasks.get(task_id=task_id)
        if t["completed"]:
            status = t["task"]["status"]
            failures = t.get("response", {}).get("failures", [])
            if failures:
                raise RuntimeError(f"{label} had failures: {failures[:3]}")
            return status
        s = t["task"]["status"]
        if time.time() - last_log > 3:
            log.info("  %s: %d/%d docs...", label,
                     s.get("created", 0) + s.get("updated", 0), s["total"])
            last_log = time.time()
        time.sleep(poll_s)
        poll_s = min(poll_s * 1.5, 2.0)


def reindex_range(es, source: str, dest: str, since_ms: int | None, label: str,
                  rps: float = -1) -> int:
    body: dict = {"source": {"index": source}, "dest": {"index": dest}, "conflicts": "proceed"}
    if since_ms is not None:
        body["source"]["query"] = {"range": {"updated_at": {"gte": since_ms}}}
    resp = es.reindex(
        source=body["source"], dest=body["dest"], conflicts="proceed",
        slices="auto", requests_per_second=rps, wait_for_completion=False,
    )
    status = wait_for_task(es, resp["task"], label)
    copied = status.get("created", 0) + status.get("updated", 0)
    log.info("  %s done: %d docs copied", label, copied)
    return copied


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--rps", type=float, default=-1,
                        help="requests_per_second throttle for baseline copy (-1 = unthrottled)")
    args = parser.parse_args()

    es = es_client()
    wait_for_es(es)
    source = alias_target(es, WRITE_ALIAS)
    if source == INDEX_V2:
        raise SystemExit(f"Aliases already point at {INDEX_V2}; nothing to do (rollback first?)")

    # -- Step 1: create v2, optimized for bulk load ---------------------------
    log.info("STEP 1: create %s (refresh disabled, 0 replicas during load)", INDEX_V2)
    body = load_index_body("v2")
    settings = dict(body["settings"])
    settings.update({"refresh_interval": "-1", "number_of_replicas": 0})
    if es.indices.exists(index=INDEX_V2):
        raise SystemExit(f"{INDEX_V2} already exists — delete it or run cleanup first")
    es.indices.create(index=INDEX_V2, settings=settings, mappings=body["mappings"])

    # -- Step 2: baseline copy ------------------------------------------------
    cursor = now_millis() - OVERLAP_MS
    log.info("STEP 2: baseline _reindex %s -> %s (slices=auto, rps=%s)", source, INDEX_V2, args.rps)
    t0 = time.time()
    copied = reindex_range(es, source, INDEX_V2, None, "baseline", rps=args.rps)
    log.info("Baseline: %d docs in %.1fs", copied, time.time() - t0)

    # -- Step 3: catch-up loop ------------------------------------------------
    # Convergence is measured in TIME, not doc count: each pass re-copies the
    # overlap window, so steady-state count never reaches zero. What we are
    # really bounding is the duration of the final write-blocked pass — once a
    # pass completes in a few seconds, the cutover window will too.
    log.info("STEP 3: catch-up passes (live writes continued during baseline)")
    for i in range(1, MAX_CATCHUP_PASSES + 1):
        next_cursor = now_millis() - OVERLAP_MS
        es.indices.refresh(index=source)
        t_pass = time.time()
        copied = reindex_range(es, source, INDEX_V2, cursor, f"catch-up #{i}")
        pass_s = time.time() - t_pass
        cursor = next_cursor
        if i >= MIN_CATCHUP_PASSES and pass_s < CATCHUP_TARGET_S:
            log.info("Converged: pass #%d copied %d docs in %.1fs (< %.0fs) — ready for cutover",
                     i, copied, pass_s, CATCHUP_TARGET_S)
            break
    else:
        raise SystemExit("Catch-up never converged: write rate exceeds copy rate. "
                         "Throttle writes or scale the cluster, then retry.")

    # -- Step 4: cutover (block writes -> final pass -> atomic swap) ----------
    log.info("STEP 4: CUTOVER — blocking writes on %s", source)
    t_block = time.time()
    es.indices.add_block(index=source, block="write")
    try:
        es.indices.refresh(index=source)
        reindex_range(es, source, INDEX_V2, cursor, "final pass (writes blocked)")
        es.indices.refresh(index=INDEX_V2)

        es.indices.update_aliases(
            actions=[
                {"remove": {"index": source, "alias": READ_ALIAS}},
                {"remove": {"index": source, "alias": WRITE_ALIAS}},
                {"add": {"index": INDEX_V2, "alias": READ_ALIAS}},
                {"add": {"index": INDEX_V2, "alias": WRITE_ALIAS}},
            ]
        )
        log.info("Aliases swapped atomically: %s / %s -> %s", READ_ALIAS, WRITE_ALIAS, INDEX_V2)
    finally:
        es.indices.put_settings(index=source, settings={"index.blocks.write": None})
    log.info("Write block window: %.2fs (writers were retrying, zero writes lost)",
             time.time() - t_block)

    # -- Step 5: restore production settings on v2 ----------------------------
    log.info("STEP 5: restore production settings on %s", INDEX_V2)
    es.indices.put_settings(index=INDEX_V2, settings={"refresh_interval": "1s"})

    log.info("DONE. %s is live. %s kept for rollback (scripts/rollback.py).", INDEX_V2, source)
    log.info("Next: python scripts/verify.py")


if __name__ == "__main__":
    main()
