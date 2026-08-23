import time

from common import read_router_state, shard_for


class GateTimeout(Exception):
    pass


class Router:
    """App-side shard map, Notion-style: the application is the single source
    of truth for routing. State lives in router_state.json, flipped atomically
    by cutover/rollback via rename()."""

    def __init__(self, gate_timeout=60.0):
        self.gate_timeout = gate_timeout

    def route_write(self, workspace_id):
        """Block while the gate is held, then resolve node AND state from the
        same post-gate snapshot — an op that entered the gate must come out
        the other side routed by the world as it is, not as it was."""
        deadline = time.monotonic() + self.gate_timeout
        while True:
            state = read_router_state()
            if not state.get("writes_gated"):
                break
            if time.monotonic() > deadline:
                raise GateTimeout("write gate held longer than gate_timeout")
            time.sleep(0.05)
        if state["authoritative"] == "mono":
            return "mono", state
        return shard_for(workspace_id), state

    def node_for_read(self, workspace_id):
        state = read_router_state()
        if state["authoritative"] == "mono":
            return "mono"
        return shard_for(workspace_id)
