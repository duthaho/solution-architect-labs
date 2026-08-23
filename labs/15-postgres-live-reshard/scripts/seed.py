import time

from common import N_ROWS, N_WORKSPACES, SEED, conn, log


def main():
    t0 = time.monotonic()
    with conn("mono") as c:
        c.execute(
            """
            INSERT INTO docs (workspace_id, title, body, updated_at)
            SELECT g %% %(nws)s,
                   'doc-' || g,
                   md5(%(seed)s::text || ':' || g) || md5(g::text),
                   now() - (g %% 86400) * interval '1 second'
            FROM generate_series(1, %(n)s) g
            """,
            {"nws": N_WORKSPACES, "seed": SEED, "n": N_ROWS},
        )
        total = c.execute("SELECT count(*) FROM docs").fetchone()[0]
        workspaces = c.execute("SELECT count(DISTINCT workspace_id) FROM docs").fetchone()[0]
    log.info(
        "seeded mono: %d rows across %d workspaces in %.1fs (seed=%d)",
        total, workspaces, time.monotonic() - t0, SEED,
    )


if __name__ == "__main__":
    main()
