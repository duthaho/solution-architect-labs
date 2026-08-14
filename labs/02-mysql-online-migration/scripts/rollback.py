"""Roll back after cutover — which is just the same migration, run backwards.

The parked _orders_old stopped receiving writes at cutover, so it is stale by
exactly the writes that hit the new table since. But those writes are all in
the binlog, and migrate.py saved the post-cutover position. So:

  1. Stream the binlog from the cutover position; apply every event on
     `orders` (the new-schema table) onto _orders_old. Column mapping drops
     ghost-only columns (currency) automatically; DECIMAL coerces back to
     FLOAT on insert. Note what this means: rolling back a WIDENING migration
     is lossy-by-schema — new-column data is discarded. Roll back early or
     roll forward.
  2. Converge on heartbeat lag, then run the identical cutover dance with the
     roles swapped: lock -> marker -> drain -> RENAME orders TO _orders_new,
     _orders_old TO orders -> unlock.

Zero writes are lost in either direction, for the same reasons as forward.
"""
import json
import time

import applier as applier_mod
from common import (
    NEW_PARKED_TABLE,
    OLD_TABLE,
    STATE_FILE,
    TABLE,
    connect,
    log,
    table_exists,
    wait_for_mysql,
)

CONVERGE_LAG_MS = 1500
CONVERGE_TIMEOUT_S = 300


def main() -> None:
    wait_for_mysql()
    conn = connect()

    if not STATE_FILE.exists():
        raise SystemExit(f"No {STATE_FILE.name} — nothing to roll back (did migrate.py finish?)")
    if not table_exists(conn, OLD_TABLE):
        raise SystemExit(f"{OLD_TABLE} does not exist — nothing to roll back")
    if table_exists(conn, NEW_PARKED_TABLE):
        raise SystemExit(f"{NEW_PARKED_TABLE} already exists — previous rollback? Drop it first.")
    state = json.loads(STATE_FILE.read_text())

    log.info("STEP 1: replay binlog since cutover (%s:%d) from %s onto %s",
             state["cutover_binlog_file"], state["cutover_binlog_pos"], TABLE, OLD_TABLE)
    applier = applier_mod.BinlogApplier(
        source_table=TABLE, target_table=OLD_TABLE,
        start_file=state["cutover_binlog_file"], start_pos=state["cutover_binlog_pos"],
        renames=[(TABLE, NEW_PARKED_TABLE), (OLD_TABLE, TABLE)],
    )
    heartbeat = applier_mod.Heartbeat()
    applier.start()
    heartbeat.start()

    log.info("STEP 2: converge (lag < %dms)", CONVERGE_LAG_MS)
    deadline = time.time() + CONVERGE_TIMEOUT_S
    while applier.lag_ms > CONVERGE_LAG_MS:
        if applier.error:
            raise SystemExit(f"Applier died: {applier.error}")
        if time.time() > deadline:
            raise SystemExit("Rollback replay never converged")
        time.sleep(0.2)
    log.info("Converged: %d events replayed onto %s, lag %.0fms",
             applier.events_applied, OLD_TABLE, applier.lag_ms)

    log.info("STEP 3: reverse cutover — lock, drain, atomic rename back")
    applier.request_cutover()
    applier.join(timeout=60)
    heartbeat.stop()
    if applier.error:
        raise SystemExit(f"Reverse cutover failed: {applier.error}")
    if not applier.cutover_done:
        raise SystemExit("Reverse cutover timed out")

    STATE_FILE.unlink()
    log.info("Rolled back: write-block window %.2fs. %s is v1 again; v2 parked as %s",
             applier.block_window_s, TABLE, NEW_PARKED_TABLE)
    log.info("Next: python scripts/verify.py  (journal replay must still pass against v1)")


if __name__ == "__main__":
    main()
