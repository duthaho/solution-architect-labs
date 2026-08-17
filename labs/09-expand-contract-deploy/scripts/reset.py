"""Restore the pristine v1 world after a drill: v1 schema (empty — the
Makefile reseeds), blue pair on v1 taking traffic, green stopped, journals
cleared. Deliberately destructive: after the naive drill the data ISN'T
recoverable (that was the lesson), so reset rebuilds instead of repairing.
"""
import common as c


def main() -> None:
    c.wait_for_mysql()
    with c.connect().cursor() as cur:
        cur.execute(f"DROP TABLE IF EXISTS {c.TABLE}")
        for stmt in [s.strip() for s in
                     (c.LAB_DIR / "sql" / "v1.sql").read_text().split(";") if s.strip()]:
            cur.execute(stmt)
    c.log("schema reset to v1 shape (empty)")

    for pod in c.PODS:
        c.write_pod_version(pod, "v1")
    c.write_upstreams(c.COLORS["blue"])
    running = [pod for pod in c.COLORS["blue"] if c.probe_pod(pod)]
    if running:
        c.compose("restart", "-t", "5", *running)
    else:
        c.compose("up", "-d", *c.COLORS["blue"])
    for pod in c.COLORS["blue"]:
        c.wait_pod_version(pod, "v1")
    c.reload_gateway()
    c.compose("stop", "-t", "5", *c.COLORS["green"], check=False)
    c.write_deploy_state({"active_color": "blue", "versions": {p: "v1" for p in c.PODS}})
    c.log("pods reset: blue pair on v1 in the LB, green stopped")

    for f in (c.BACKFILL_STATE, c.CLIENT_JOURNAL, c.CLIENT_STATE):
        if f.exists():
            f.unlink()
    c.log("state files cleared — reseed with: make seed")


if __name__ == "__main__":
    main()
