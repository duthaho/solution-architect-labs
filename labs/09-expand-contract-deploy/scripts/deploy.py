"""The deploy system: rolling and blue-green, over two boring primitives
(write a file, restart a container) plus one traffic switch (nginx reload).

rolling   Replace pods of the ACTIVE color one at a time, each drained from
          the LB before restart and health-checked back in after:

              remove pod from upstreams -> reload -> drain 0.5s
              write version file -> restart container -> wait /healthz == v
              add pod back -> reload -> pause

          Zero downtime BY ITSELF — but for the whole --pause window, old-code
          and new-code pods serve side by side against one schema. That window
          is what this lab is about.

bluegreen Boot the IDLE color at the new version, health-check it while it
          receives no traffic, then flip the upstream set in ONE reload.
          The mixed-version window at the traffic layer is ~0; the old color
          keeps running, drained, as the instant-rollback path. What blue-green
          can NOT do is flip the database — both colors share it, which is why
          the schema still has to move expand->contract underneath.

State is journaled to deploy_state.json after every pod (atomic rename), so a
crashed deploy resumes where it stopped. `status` asks the pods themselves —
the state file is intent; /healthz is truth.
"""
import argparse
import sys
import time

import common as c


def other(color: str) -> str:
    return "green" if color == "blue" else "blue"


def rolling(version: str, pause: float) -> None:
    state = c.read_deploy_state()
    pods = c.COLORS[state["active_color"]]
    lb = c.in_lb()
    assert set(lb) == set(pods), f"LB {lb} != active color pods {pods} — fix state first"
    c.log(f"ROLLING {state['active_color']} -> {version} "
          f"(pause {pause}s between pods)")
    for pod in pods:
        c.write_upstreams([p for p in pods if p != pod])
        c.reload_gateway()
        time.sleep(0.5)  # drain: in-flight requests finish before the restart
        c.write_pod_version(pod, version)
        c.compose("restart", "-t", "5", pod)
        c.wait_pod_version(pod, version)
        c.write_upstreams(pods)
        c.reload_gateway()
        state["versions"][pod] = version
        c.write_deploy_state(state)
        c.log(f"  {pod}: now serving {version}, back in LB")
        if pod != pods[-1]:
            time.sleep(pause)  # the mixed-version window, made explicit
    c.log(f"ROLLING done: {', '.join(pods)} all {version}")


def bluegreen(version: str, stop_old: bool) -> None:
    state = c.read_deploy_state()
    old, new = state["active_color"], other(state["active_color"])
    pods = c.COLORS[new]
    c.log(f"BLUE-GREEN: booting {new} pair at {version} (no traffic yet)")
    for pod in pods:
        c.write_pod_version(pod, version)
    c.compose("up", "-d", *pods)
    c.compose("restart", "-t", "5", *pods)  # force a fresh boot -> re-read version
    for pod in pods:
        c.wait_pod_version(pod, version)
        state["versions"][pod] = version
    c.log(f"  {new} pair healthy at {version} — flipping traffic in one reload")
    c.write_upstreams(pods)
    c.reload_gateway()
    state["active_color"] = new
    c.write_deploy_state(state)
    c.log(f"BLUE-GREEN done: {new} active; {old} kept running as rollback path"
          + (" (stopping it)" if stop_old else ""))
    if stop_old:
        c.compose("stop", "-t", "5", *c.COLORS[old])


def status() -> int:
    state = c.read_deploy_state()
    lb = set(c.in_lb())
    print(f"active color: {state['active_color']}   in LB: {sorted(lb)}")
    for pod in c.PODS:
        live = c.probe_pod(pod)
        print(f"  {pod:12s} intent={state['versions'].get(pod, '?'):4s} "
              f"live={live or 'DOWN':4s} {'<- traffic' if pod in lb else ''}")
    return 0


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("command", nargs="?", default="deploy", choices=["deploy", "status"])
    ap.add_argument("--strategy", choices=["rolling", "bluegreen"])
    ap.add_argument("--version", choices=c.VERSIONS)
    ap.add_argument("--pause", type=float, default=2.0,
                    help="rolling: seconds between pods (the mixed-version window)")
    ap.add_argument("--stop-old", action="store_true",
                    help="bluegreen: stop the old color (forfeit instant rollback)")
    args = ap.parse_args()

    if args.command == "status":
        return status()
    if not args.strategy or not args.version:
        ap.error("deploy requires --strategy and --version")
    if args.strategy == "rolling":
        rolling(args.version, args.pause)
    else:
        bluegreen(args.version, args.stop_old)
    return 0


if __name__ == "__main__":
    sys.exit(main())
