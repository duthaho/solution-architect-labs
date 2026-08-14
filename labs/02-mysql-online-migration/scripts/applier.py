"""Binlog streaming applier + atomic cutover — the heart of the migration.

This is the gh-ost idea in ~200 lines: subscribe to the server's own
replication stream (row-based binlog), and replay every change to the source
table onto the target table while the backfill runs in parallel.

Apply semantics (the "who wins" rules that make concurrent backfill safe):
  INSERT/UPDATE events -> REPLACE INTO target (full row image)
  DELETE events        -> DELETE FROM target WHERE pk = ...
  backfill (elsewhere) -> INSERT IGNORE ... SELECT ... FOR SHARE

Binlog data is always newer than backfill data for the same row, so binlog
writes must overwrite (REPLACE) while backfill must never overwrite
(INSERT IGNORE). See the README for the full correctness argument, including
the delete race that FOR SHARE closes.

Cutover (MySQL >= 8.0.13) runs INSIDE this thread, because the session that
holds the table locks must be the same session that applies the final events
and executes the rename:

  1. LOCK TABLES source WRITE, target WRITE   -- app writes now queue on MDL
  2. insert a marker row into the changelog   -- via a second connection
  3. keep applying the stream until the marker arrives -> target is EXACTLY
     source (every event before the marker was committed before the lock)
  4. RENAME TABLE source TO parked, target TO source  -- atomic, allowed
     under LOCK TABLES since 8.0.13 because we hold WRITE locks on both
  5. UNLOCK TABLES -- queued app writes proceed against the new table

Lag is measured gh-ost-style: the orchestrator inserts heartbeat rows into a
changelog table; the applier records the timestamp of the last heartbeat it
has APPLIED THROUGH. now - that = end-to-end replication lag, measured through
the actual pipeline instead of trusting any secondary metric.
"""
import threading
import time
import uuid

from pymysqlreplication import BinLogStreamReader
from pymysqlreplication.row_event import DeleteRowsEvent, UpdateRowsEvent, WriteRowsEvent

from common import (
    CHANGELOG_TABLE,
    DB,
    binlog_position,
    connect,
    connection_settings,
    log,
    now_millis,
    table_columns,
)

SERVER_ID = 4379  # must differ from the server's own server-id
LOCK_WAIT_TIMEOUT_S = 10  # fail the cutover fast instead of queueing writes behind us
CUTOVER_RETRIES = 3


class BinlogApplier(threading.Thread):
    """Streams row events for `source_table` and applies them to `target_table`.

    `renames` is the atomic cutover statement's table mapping, e.g.
    [(source, parked_name), (target, source)].
    """

    def __init__(self, source_table: str, target_table: str,
                 start_file: str, start_pos: int, renames: list[tuple[str, str]]):
        super().__init__(daemon=True, name="binlog-applier")
        self.source_table = source_table
        self.target_table = target_table
        self.renames = renames
        self.start_file = start_file
        self.start_pos = start_pos

        # Columns present in BOTH tables, in source order. Ghost-only columns
        # (e.g. a new column with a default) are filled by the target's
        # defaults; source-only columns (dropped by the migration) are ignored.
        conn = connect()
        src_cols = table_columns(conn, source_table)
        tgt_cols = set(table_columns(conn, target_table))
        conn.close()
        self.shared_cols = [c for c in src_cols if c in tgt_cols]

        self.events_applied = 0
        self.lag_ms: float = float("inf")
        self.error: BaseException | None = None
        self.cutover_done = False
        self.block_window_s: float | None = None
        self.post_cutover_position: tuple[str, int] | None = None
        self._cutover_requested = threading.Event()
        self._marker: str | None = None

    # -- public API -----------------------------------------------------------

    def request_cutover(self) -> None:
        self._cutover_requested.set()

    # -- internals ------------------------------------------------------------

    def _replace_rows(self, cur, rows: list[dict]) -> None:
        cols = ", ".join(f"`{c}`" for c in self.shared_cols)
        placeholders = ", ".join(["%s"] * len(self.shared_cols))
        cur.executemany(
            f"REPLACE INTO `{self.target_table}` ({cols}) VALUES ({placeholders})",
            [tuple(row[c] for c in self.shared_cols) for row in rows],
        )

    def _apply(self, cur, event) -> None:
        if isinstance(event, WriteRowsEvent):
            self._replace_rows(cur, [r["values"] for r in event.rows])
        elif isinstance(event, UpdateRowsEvent):
            self._replace_rows(cur, [r["after_values"] for r in event.rows])
        elif isinstance(event, DeleteRowsEvent):
            cur.executemany(
                f"DELETE FROM `{self.target_table}` WHERE id = %s",
                [(r["values"]["id"],) for r in event.rows],
            )
        self.events_applied += len(event.rows)

    def _lock_both_tables(self, cur) -> float:
        """Acquire WRITE locks with retries. Returns the lock-acquired timestamp.

        A long-running query on the source table would make LOCK TABLES wait,
        and every app write queues up BEHIND our lock request while it waits
        (MDL is a fair queue). A short lock_wait_timeout + retry turns that
        production incident into a failed attempt.
        """
        cur.execute(f"SET SESSION lock_wait_timeout = {LOCK_WAIT_TIMEOUT_S}")
        for attempt in range(1, CUTOVER_RETRIES + 1):
            try:
                cur.execute(
                    f"LOCK TABLES `{self.source_table}` WRITE, `{self.target_table}` WRITE")
                return time.time()
            except Exception as e:
                log.warning("Cutover lock attempt %d/%d failed: %s", attempt, CUTOVER_RETRIES, e)
                if attempt == CUTOVER_RETRIES:
                    raise
                time.sleep(1)
        raise RuntimeError("unreachable")

    def _finalize_cutover(self, apply_cur, ctrl_conn, t_locked: float) -> None:
        # Carry the AUTO_INCREMENT counter over. CREATE TABLE ... LIKE does not
        # copy it, and backfilled ids only push it to max(id)+1 — if the rows
        # with the highest ids were deleted during the migration, the new table
        # would re-issue their ids. Classic gh-ost gotcha, fixed under the lock.
        with ctrl_conn.cursor() as c:
            c.execute(
                "SELECT AUTO_INCREMENT FROM information_schema.tables "
                "WHERE table_schema=%s AND table_name=%s", (DB, self.source_table))
            auto_inc = c.fetchone()[0]
        if auto_inc:
            apply_cur.execute(f"ALTER TABLE `{self.target_table}` AUTO_INCREMENT = {auto_inc}")

        renames = ", ".join(f"`{a}` TO `{b}`" for a, b in self.renames)
        apply_cur.execute(f"RENAME TABLE {renames}")

        # Everything after this binlog position belongs to the NEW table only.
        # rollback.py replays from here.
        self.post_cutover_position = binlog_position(ctrl_conn)

        apply_cur.execute("UNLOCK TABLES")
        self.block_window_s = time.time() - t_locked
        self.cutover_done = True

    def run(self) -> None:
        stream = BinLogStreamReader(
            connection_settings=connection_settings(),
            server_id=SERVER_ID,
            blocking=True,
            resume_stream=True,
            log_file=self.start_file,
            log_pos=self.start_pos,
            only_events=[WriteRowsEvent, UpdateRowsEvent, DeleteRowsEvent],
            only_schemas=[DB],
            only_tables=[self.source_table, CHANGELOG_TABLE],
        )
        apply_conn = connect()
        ctrl_conn = connect()  # for statements a LOCK TABLES session may not run
        locked = False
        t_locked = 0.0
        try:
            with apply_conn.cursor() as cur:
                for event in stream:
                    if event.table == self.source_table:
                        self._apply(cur, event)
                    elif event.table == CHANGELOG_TABLE and isinstance(
                            event, (WriteRowsEvent, UpdateRowsEvent)):
                        for row in event.rows:
                            vals = row.get("values") or row.get("after_values")
                            if vals["kind"] == "heartbeat":
                                self.lag_ms = now_millis() - int(vals["value"])
                            elif vals["kind"] == "marker" and vals["value"] == self._marker:
                                # Drained: every source event committed before
                                # the lock has been applied. Cut over.
                                self._finalize_cutover(cur, ctrl_conn, t_locked)
                                return

                    if self._cutover_requested.is_set() and not locked:
                        # From here on, app writes to the source table queue up.
                        # Everything until the rename must be tight.
                        t_locked = self._lock_both_tables(cur)
                        locked = True
                        self._marker = uuid.uuid4().hex
                        with ctrl_conn.cursor() as c:
                            c.execute(
                                f"INSERT INTO {CHANGELOG_TABLE} (kind, value) VALUES ('marker', %s)",
                                (self._marker,))
        except BaseException as e:  # surface thread failures to the orchestrator
            self.error = e
            log.error("Applier failed: %s", e)
        finally:
            try:
                if locked and not self.cutover_done:
                    with apply_conn.cursor() as cur:
                        cur.execute("UNLOCK TABLES")
            except Exception:
                pass
            stream.close()
            apply_conn.close()
            ctrl_conn.close()


class Heartbeat(threading.Thread):
    """Inserts a timestamp row into the changelog twice a second. The applier's
    lag reading is `now - last heartbeat applied` — measured through the same
    binlog pipeline as the data, so it can't lie."""

    def __init__(self, interval_s: float = 0.5):
        super().__init__(daemon=True, name="heartbeat")
        self._stop = threading.Event()
        self.interval_s = interval_s

    def stop(self) -> None:
        self._stop.set()

    def run(self) -> None:
        conn = connect()
        try:
            while not self._stop.is_set():
                with conn.cursor() as cur:
                    cur.execute(
                        f"INSERT INTO {CHANGELOG_TABLE} (kind, value) VALUES ('heartbeat', %s)",
                        (str(now_millis()),))
                self._stop.wait(self.interval_s)
        finally:
            conn.close()
