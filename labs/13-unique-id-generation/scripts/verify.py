"""The invariant gate. Joins every hardened journal and the lease events.

Inputs are explicit:
  - all ids_*.jsonl journals EXCEPT those tagged naive (ids_*naive*.jsonl)
  - lease_events.jsonl

Asserts:
  1. Global uniqueness — no id appears twice across all included journals.
  2. Per-worker strict monotonicity within each journal (append order).
  3. Layout round-trip — decode(id) matches the journaled (ts, worker, seq).
  4. Lease exclusivity — for a given worker id, no two owners' held windows
     ([claim, max expires_at] or release, whichever ends first) overlap.

VERIFY_INVERT=1 flips the contract: it inspects ONLY the naive journals and
exits 0 iff they contain violations — proof the gate catches the bug rather
than vacuously passing. Exit 1 if there is nothing to check: an empty gate
is not a passing gate.
"""

import os
import sys
from collections import defaultdict

import common
import snowflake

INVERT = os.environ.get("VERIFY_INVERT") == "1"


def id_journals(naive: bool) -> list:
    paths = sorted(common.LAB_DIR.glob("ids_*.jsonl"))
    return [p for p in paths if ("naive" in p.name) == naive]


def check_ids(paths) -> list[str]:
    failures: list[str] = []
    seen: dict[int, str] = {}
    total = 0
    for path in paths:
        rows = common.read_jsonl(path)
        total += len(rows)
        last_by_worker: dict[int, int] = {}
        for i, r in enumerate(rows):
            id_ = r["id"]
            # 1. global uniqueness
            if id_ in seen:
                failures.append(f"{path.name}:{i}: id {id_} already in {seen[id_]}")
            else:
                seen[id_] = path.name
            # 3. layout round-trip
            if snowflake.decode(id_) != (r["ts"], r["worker"], r["seq"]):
                failures.append(f"{path.name}:{i}: decode mismatch for id {id_}")
            # 2. per-worker monotonicity within the journal
            w = r["worker"]
            if w in last_by_worker and id_ <= last_by_worker[w]:
                failures.append(
                    f"{path.name}:{i}: worker {w} id {id_} not > previous"
                )
            last_by_worker[w] = id_
    if total == 0:
        failures.append("no ids to verify — the gate refuses to pass vacuously")
    return failures


def check_leases() -> list[str]:
    """4. No two owners ever held the same worker id at once."""
    events = common.read_jsonl(common.LEASE_EVENTS_PATH)
    # A "session" = one owner's tenure on one worker id, keyed by claim order.
    sessions: dict[tuple[int, str], list[dict]] = defaultdict(list)
    intervals: dict[int, list[tuple[int, int, str]]] = defaultdict(list)
    for e in events:
        key = (e["worker_id"], e["owner"])
        if e["event"] == "claim":
            sessions[key].append({"start": e["at"], "end": e["expires_at"]})
        elif e["event"] == "renew" and sessions[key]:
            sessions[key][-1]["end"] = max(sessions[key][-1]["end"], e["expires_at"])
        elif e["event"] == "release" and sessions[key]:
            sessions[key][-1]["end"] = min(sessions[key][-1]["end"], e["at"])
    for (wid, owner), sess in sessions.items():
        for s in sess:
            intervals[wid].append((s["start"], s["end"], owner))
    failures = []
    for wid, ivs in intervals.items():
        # All pairs, not just sort-adjacent ones: a long first interval can
        # overlap an interval that isn't its immediate successor.
        for i in range(len(ivs)):
            s1, e1, o1 = ivs[i]
            for s2, e2, o2 in ivs[i + 1:]:
                if o1 != o2 and max(s1, s2) < min(e1, e2):
                    failures.append(
                        f"worker id {wid}: {o1} [{s1},{e1}) overlaps "
                        f"{o2} [{s2},{e2})"
                    )
    return failures


def main() -> int:
    if INVERT:
        paths = id_journals(naive=True)
        failures = check_ids(paths)
        for f in failures:
            common.log.info("caught (expected): %s", f)
        dupes = [f for f in failures if "already in" in f]
        if dupes:
            common.log.info(
                "verify(INVERT): naive journals violate uniqueness (%d) — "
                "the gate catches the bug — exit 0", len(dupes),
            )
            return 0
        common.log.error("verify(INVERT): naive journals passed — gate is vacuous")
        return 1

    paths = id_journals(naive=False)
    failures = check_ids(paths) + check_leases()
    for f in failures:
        common.log.error("violation: %s", f)
    n_ids = sum(len(common.read_jsonl(p)) for p in paths)
    if not failures:
        common.log.info(
            "verify: %d ids across %d journals unique, monotonic, decodable; "
            "lease windows exclusive — exit 0", n_ids, len(paths),
        )
        return 0
    common.log.error("verify: %d violations", len(failures))
    return 1


if __name__ == "__main__":
    sys.exit(main())
