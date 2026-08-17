"""The naive migration: everything in one shot, the way it looks in a PR that
says "rename users.name to first/last — small change, ran fine in staging".

    1. ADD first_name, last_name
    2. one big UPDATE to copy every row        <- v1 keeps writing `name` DURING
    3. DROP name                                  and AFTER this copy...

Step 3 is the moment acked writes die: any `name` write that landed after the
copy touched its row exists ONLY in `name` — and DROP discards it. No error
was returned to anyone. The client's audit will find the bodies.

After this script, the drill rolls pods v1 -> v3; until each pod restarts, the
still-running v1 code 500s on every request (`Unknown column 'name'`). That is
the visible half of the damage. The invisible half is the lost writes.
"""
import time

import common as c


def main() -> None:
    with c.connect().cursor() as cur:
        c.log("naive 1/3: ADD COLUMN first_name, last_name")
        if not {"first_name", "last_name"} <= c.columns():
            cur.execute(f"ALTER TABLE {c.TABLE} "
                        "ADD COLUMN first_name VARCHAR(255) NULL, "
                        "ADD COLUMN last_name VARCHAR(255) NULL, "
                        "ALGORITHM=INSTANT")

        c.log("naive 2/3: one big UPDATE to copy every row (v1 is still writing name...)")
        t0 = time.time()
        cur.execute(f"UPDATE {c.TABLE} SET first_name = {c.SQL_FIRST}, "
                    f"last_name = {c.SQL_LAST}")
        c.log(f"          copied {cur.rowcount} rows in {time.time() - t0:.1f}s")

        # The gap between "copy finished" and "column dropped": in a real
        # migration this is minutes of a human reading the runbook. 2s is
        # enough for the client to land acked writes that are about to die.
        time.sleep(2)

        c.log("naive 3/3: DROP COLUMN name  <- acked writes since the copy die here")
        cur.execute(f"ALTER TABLE {c.TABLE} DROP COLUMN name")
    c.log("naive migration done — the schema is v3-shaped, the pods are still v1")


if __name__ == "__main__":
    main()
