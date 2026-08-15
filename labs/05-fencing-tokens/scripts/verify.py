"""Read the ledger, check the invariant, print the post-mortem timeline.

The invariant
-------------
A critical section = all appends made under one (owner, token) pair. Mutual
exclusion means each critical section's entries must be CONTIGUOUS in the
ledger: if any other owner's entry appears between the first and last entry of
a session, two workers were inside the "exclusive" section at the same time.

That contiguity check is the entire detector. No timing heuristics — the
ledger's arrival order is the ground truth (single-threaded storage assigns
idx at accept time).
"""
import argparse
import sys

from common import ledger


def analyze(state: dict) -> dict:
    entries = state["entries"]
    # session key -> list of ledger indices, in order
    sessions: dict[tuple, list[int]] = {}
    for e in entries:
        sessions.setdefault((e["owner"], e["token"]), []).append(e["idx"])

    violations = []
    for (owner, token), idxs in sessions.items():
        span = entries[idxs[0]:idxs[-1] + 1]
        foreign = [e for e in span if (e["owner"], e["token"]) != (owner, token)]
        if foreign:
            violations.append({"owner": owner, "token": token,
                               "span": (idxs[0], idxs[-1]),
                               "foreign": foreign})

    return {
        "fencing": state["fencing"],
        "corrupt": bool(violations),
        "violations": violations,
        "n_entries": len(entries),
        "n_sessions": len(sessions),
        "rejected": state["rejected"],
    }


def print_report(state: dict, verdict: dict) -> None:
    entries = state["entries"]
    t0 = entries[0]["ts_ms"] if entries else 0
    # indices that sit inside someone else's critical section
    tainted = {f["idx"] for v in verdict["violations"] for f in v["foreign"]}

    print(f"\n  ledger timeline (fencing={'on' if state['fencing'] else 'off'}, "
          f"{len(entries)} entries):")
    print("   idx   t+ms  owner  token  seq")
    for e in entries:
        mark = "  ⚠ INTERLEAVED" if e["idx"] in tainted else ""
        print(f"  {e['idx']:>4} {e['ts_ms'] - t0:>6}    {e['owner']}    "
              f"{e['token']:>4}  {e['seq']:>3}{mark}")

    for r in verdict["rejected"]:
        print(f"  409 {r['ts_ms'] - t0:>6}    {r['owner']}    {r['token']:>4}  "
              f"{r['seq']:>3}   REJECTED: {r['reason']}")

    if verdict["corrupt"]:
        print("\n  ✗ INVARIANT VIOLATED — critical sections interleaved:")
        for v in verdict["violations"]:
            others = ", ".join(f"{f['owner']}(token={f['token']},seq={f['seq']})"
                               for f in v["foreign"])
            print(f"    owner {v['owner']} token={v['token']} spans ledger "
                  f"idx {v['span'][0]}..{v['span'][1]}, but contains: {others}")
        print("    Both workers were inside the 'mutually exclusive' section. "
              "The lock lied.")
    else:
        print("\n  ✓ invariant holds: every critical section is contiguous "
              f"({verdict['n_sessions']} sessions, "
              f"{len(verdict['rejected'])} stale write(s) fenced off)")


def run(expect: str | None = None) -> dict:
    state = ledger()
    verdict = analyze(state)
    print_report(state, verdict)
    if expect == "corrupt" and not verdict["corrupt"]:
        raise SystemExit("expected corruption, ledger is clean")
    if expect == "clean" and verdict["corrupt"]:
        raise SystemExit("expected clean ledger, found corruption")
    return verdict


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--expect", choices=["corrupt", "clean"])
    args = parser.parse_args()
    verdict = run(args.expect)
    sys.exit(0 if (args.expect or not verdict["corrupt"]) else 1)
