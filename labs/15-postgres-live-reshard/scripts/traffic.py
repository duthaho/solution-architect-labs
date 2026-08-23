import os
import signal
import sys
import time

from common import JOURNAL, N_ROWS, N_WORKSPACES, append_jsonl, conn, log, read_router_state
from router import Router

RATE_SLEEP = float(os.environ.get("TRAFFIC_SLEEP", "0.005"))
TRAFFIC_SECONDS = float(os.environ.get("TRAFFIC_SECONDS", "0"))
FORCE_INSERTS = os.environ.get("TRAFFIC_FORCE_INSERTS") == "1"

_stop = False


def _handle_stop(signum, frame):
    global _stop
    _stop = True


class Traffic:
    def __init__(self):
        self.router = Router()
        self.conns = {}
        self.rng = __import__("random").Random(f"{os.environ.get('SEED', '1503')}:traffic")
        self.acked = 0

    def _conn(self, node):
        if node not in self.conns:
            self.conns[node] = conn(node)
        return self.conns[node]

    def _drop(self, node):
        try:
            self.conns.pop(node).close()
        except Exception:
            pass

    def one_op(self):
        ws = self.rng.randrange(N_WORKSPACES)
        # The insert path mints ids from the local sequence; until the
        # post-cutover sequence fix (drill_sequence) the shard sequences
        # collide with existing ids, so inserts stay on the runbook's
        # "frozen until sequences fixed" rule unless a drill forces them.
        insert_ok = read_router_state()["authoritative"] == "mono" or FORCE_INSERTS
        do_insert = insert_ok and self.rng.random() < 0.3
        if do_insert:
            node = self.router.node_for_write(ws)
            c = self._conn(node)
            row = c.execute(
                "INSERT INTO docs (workspace_id, title, body) "
                "VALUES (%s, 'live-' || clock_timestamp(), md5(random()::text)) "
                "RETURNING id, rev",
                (ws,),
            ).fetchone()
        else:
            doc_id = self.rng.randrange(1, N_ROWS + 1)
            ws = doc_id % N_WORKSPACES
            node = self.router.node_for_write(ws)
            c = self._conn(node)
            row = c.execute(
                "UPDATE docs SET rev = rev + 1, updated_at = now() "
                "WHERE workspace_id = %s AND id = %s RETURNING id, rev",
                (ws, doc_id),
            ).fetchone()
            if row is None:
                return
        append_jsonl(
            JOURNAL,
            {
                "op": "insert" if do_insert else "update",
                "ws": ws,
                "id": row[0],
                "rev": row[1],
                "node": node,
                "ts": time.time(),
            },
        )
        self.acked += 1

    def run(self, stop_event=None, seconds=None):
        seconds = TRAFFIC_SECONDS if seconds is None else seconds
        deadline = time.monotonic() + seconds if seconds else None
        log.info("traffic started (sleep=%s, seconds=%s)", RATE_SLEEP, seconds or "∞")
        while not _stop and not (stop_event and stop_event.is_set()) and (
            deadline is None or time.monotonic() < deadline
        ):
            try:
                self.one_op()
            except KeyboardInterrupt:
                break
            except Exception as e:
                for n in list(self.conns):
                    self._drop(n)
                log.warning("op failed (%s: %s) — reconnecting", type(e).__name__, e)
                time.sleep(0.2)
            time.sleep(RATE_SLEEP)
        log.info("traffic stopped: %d acked writes journaled", self.acked)


def main():
    signal.signal(signal.SIGTERM, _handle_stop)
    signal.signal(signal.SIGINT, _handle_stop)
    Traffic().run()


if __name__ == "__main__":
    main()
