"""The resource the lock is supposed to protect: an append-only ledger over HTTP.

This service deliberately knows NOTHING about Redis, TTLs, or who "holds the
lock". That is the whole point: correctness must not depend on clients being
honest about lock ownership, because a paused client *is* honest — it just has
stale beliefs. The only thing storage can trust is what arrives in the request.

Two modes:

  --fencing off   accept every append (what most systems run — and why they
                  corrupt data when a lock-holder pauses past its TTL)
  --fencing on    track the highest fencing token ever seen; reject any append
                  whose token is LOWER with 409 Conflict. Equal is fine — the
                  same holder writes many times under one token. This check is
                  one integer comparison; the entire fix is ~10 lines.

Endpoints:
  POST /append   {"owner": "A", "token": 3, "seq": 1}  -> 200 | 409
  GET  /ledger   full state: entries, rejected attempts, max_token
  GET  /health   liveness

Single-threaded HTTP server: requests serialize, so max_token check-and-update
is atomic without explicit locking. (In production this lives wherever your
storage already serializes writes — a row lock, a CAS loop, a conditional PUT.)
"""
import argparse
import json
import time
from http.server import BaseHTTPRequestHandler, HTTPServer

STATE = {
    "fencing": False,
    "max_token": 0,
    "entries": [],    # accepted appends, in arrival order
    "rejected": [],   # fenced-off attempts, for the post-mortem timeline
}


class Handler(BaseHTTPRequestHandler):

    def _reply(self, code: int, body: dict) -> None:
        data = json.dumps(body).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def do_GET(self):
        if self.path == "/health":
            self._reply(200, {"ok": True})
        elif self.path == "/ledger":
            self._reply(200, STATE)
        else:
            self._reply(404, {"error": "not found"})

    def do_POST(self):
        if self.path != "/append":
            self._reply(404, {"error": "not found"})
            return
        length = int(self.headers.get("Content-Length", 0))
        req = json.loads(self.rfile.read(length))
        entry = {
            "idx": len(STATE["entries"]),
            "ts_ms": int(time.time() * 1000),
            "owner": req["owner"],
            "token": int(req["token"]),
            "seq": int(req["seq"]),
        }

        if STATE["fencing"] and entry["token"] < STATE["max_token"]:
            entry["reason"] = f"token {entry['token']} < max_seen {STATE['max_token']}"
            STATE["rejected"].append(entry)
            self._reply(409, {"error": "stale fencing token", **entry})
            return

        STATE["max_token"] = max(STATE["max_token"], entry["token"])
        STATE["entries"].append(entry)
        self._reply(200, entry)

    def log_message(self, fmt, *args):  # quiet: the ledger IS the log
        pass


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--fencing", choices=["on", "off"], default="off")
    parser.add_argument("--port", type=int, default=8091)
    args = parser.parse_args()

    STATE["fencing"] = args.fencing == "on"
    print(f"storage: listening on :{args.port}, fencing={args.fencing}", flush=True)
    HTTPServer(("0.0.0.0", args.port), Handler).serve_forever()


if __name__ == "__main__":
    main()
