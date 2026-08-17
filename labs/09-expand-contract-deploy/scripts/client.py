"""The truth-teller: continuous traffic through the gateway, every response
asserted, every anomaly journaled with a timestamp.

This client deliberately does NOT retry. Retries are how real incidents hide;
here every failed or wrong response is evidence, and the drills are graded on
this journal. Each request uses a fresh connection (Connection: close) so a
gateway reload can never half-kill a reused socket and muddy the numbers —
what lands in the journal is the app's behavior, not TCP archaeology.

What it asserts, per op:
  create  201 with an id                        -> remember id -> name (acked)
  update  200 on an id we created               -> remember new name (acked)
  read    the name equals the LAST ACKED write  -> else `mismatch`
          (reads of seeded ids assert shape only: non-null, non-empty
           -> else `null_name` — that's a data gap surfacing)

On shutdown it dumps {id: last_acked_name} to client_state.json so verify.py
can audit the database against every write the app ever acknowledged — the
"acked but lost" count, same idea as lab 04's RPO measurement.

Anomaly kinds: http_error (5xx), conn_error, mismatch, null_name, lost_row
(404 on an id that was acked). FAILURES = sum of all of them.
"""
import http.client
import json
import random
import signal
import threading
import time

import common as c

N_THREADS = 3
OP_SLEEP_S = 0.015

stop = threading.Event()
journal_lock = threading.Lock()
stats_lock = threading.Lock()
stats = {"ops": 0, "http_error": 0, "conn_error": 0, "mismatch": 0,
         "null_name": 0, "lost_row": 0}
acked_maps: list[dict] = []


def journal(kind: str, **fields) -> None:
    with stats_lock:
        stats[kind] += 1
    entry = {"ts_ms": int(time.time() * 1000), "kind": kind, **fields}
    with journal_lock:
        with open(c.CLIENT_JOURNAL, "a") as f:
            f.write(json.dumps(entry) + "\n")


def request(method: str, path: str, body: dict | None = None):
    """-> (status, payload) or raises. Fresh connection per request."""
    conn = http.client.HTTPConnection("127.0.0.1", c.GATEWAY_PORT, timeout=8)
    try:
        payload = json.dumps(body) if body is not None else None
        conn.request(method, path, body=payload,
                     headers={"Content-Type": "application/json", "Connection": "close"})
        resp = conn.getresponse()
        data = resp.read()
        return resp.status, (json.loads(data) if data else {})
    finally:
        conn.close()


def worker(tid: int, rng: random.Random) -> None:
    acked: dict[int, str] = {}
    acked_maps.append(acked)
    names = ["Ada Lovelace", "Grace Hopper", "Alan Turing", "Barbara Liskov",
             "Leslie Lamport", "Radia Perlman", "Margaret Hamilton", "Vint Cerf"]

    while not stop.is_set():
        time.sleep(OP_SLEEP_S)
        with stats_lock:
            stats["ops"] += 1
        r = rng.random()
        try:
            if r < 0.20 or not acked:  # create
                name = f"{rng.choice(names).split()[0]} T{tid}n{rng.randrange(10**6)}"
                status, resp = request("POST", "/users", {"name": name})
                if status == 201 and "id" in resp:
                    acked[resp["id"]] = name
                else:
                    journal("http_error", op="create", status=status,
                            detail=str(resp)[:200])
            elif r < 0.50:  # update an id we own
                uid = rng.choice(list(acked))
                name = f"{rng.choice(names).split()[0]} T{tid}n{rng.randrange(10**6)}"
                status, resp = request("PUT", f"/users/{uid}", {"name": name})
                if status == 200:
                    acked[uid] = name
                elif status == 404:
                    journal("lost_row", op="update", id=uid, expected=acked[uid])
                else:
                    journal("http_error", op="update", id=uid, status=status,
                            detail=str(resp)[:200])
            elif r < 0.90:  # read an id we own: full round-trip assertion
                uid = rng.choice(list(acked))
                status, resp = request("GET", f"/users/{uid}")
                if status == 200:
                    if resp.get("name") != acked[uid]:
                        kind = "null_name" if resp.get("name") in (None, "") else "mismatch"
                        journal(kind, op="read", id=uid,
                                expected=acked[uid], got=resp.get("name"))
                elif status == 404:
                    journal("lost_row", op="read", id=uid, expected=acked[uid])
                else:
                    journal("http_error", op="read", id=uid, status=status,
                            detail=str(resp)[:200])
            else:  # read a random seeded row: shape assertion only
                uid = rng.randrange(1, c.SEED_ROWS + 1)
                status, resp = request("GET", f"/users/{uid}")
                if status == 200:
                    if resp.get("name") in (None, ""):
                        journal("null_name", op="read_seeded", id=uid,
                                got=resp.get("name"))
                elif status != 404:  # 404 tolerated: reset may have reseeded fewer
                    journal("http_error", op="read_seeded", id=uid, status=status,
                            detail=str(resp)[:200])
        except Exception as e:
            journal("conn_error", detail=f"{type(e).__name__}: {e}"[:200])


def main() -> None:
    # Fresh evidence per run: each traffic session grades exactly one scenario.
    c.CLIENT_JOURNAL.write_text("")
    if c.CLIENT_STATE.exists():
        c.CLIENT_STATE.unlink()

    signal.signal(signal.SIGTERM, lambda *_: stop.set())
    signal.signal(signal.SIGINT, lambda *_: stop.set())

    threads = [threading.Thread(target=worker, args=(i, random.Random(100 + i)))
               for i in range(N_THREADS)]
    for t in threads:
        t.start()
    c.log(f"traffic: {N_THREADS} threads against {c.GATEWAY_URL}")

    last = 0
    while not stop.is_set():
        stop.wait(10)
        with stats_lock:
            snapshot = dict(stats)
        failures = sum(v for k, v in snapshot.items() if k != "ops")
        c.log(f"progress: {snapshot['ops']} ops (+{snapshot['ops'] - last}), "
              f"{failures} failures so far {snapshot}")
        last = snapshot["ops"]

    for t in threads:
        t.join()

    merged = {}
    for m in acked_maps:
        merged.update(m)
    c.CLIENT_STATE.write_text(json.dumps(
        {str(k): v for k, v in merged.items()}, indent=0) + "\n")

    with stats_lock:
        snapshot = dict(stats)
    failures = sum(v for k, v in snapshot.items() if k != "ops")
    print(f"TRAFFIC SUMMARY: {snapshot['ops']} ops, {failures} FAILURES, "
          f"http={snapshot['http_error']} conn={snapshot['conn_error']} "
          f"mismatch={snapshot['mismatch']} null={snapshot['null_name']} "
          f"lost={snapshot['lost_row']} (acked writes tracked: {len(merged)})",
          flush=True)


if __name__ == "__main__":
    main()
