"""EXPAND: add the new columns. Additive, nullable, INSTANT — the only kind of
schema change that is safe to ship while old code is still running.

Why nullable, when the end state wants them NOT NULL? Because a NOT NULL
column would force a DEFAULT on every existing row (a lie) or break v1's
INSERTs (which don't mention the column). Nullability IS the compatibility
window; the constraint tightens only after contract, when nothing old is left.

Why INSTANT matters: this ALTER takes a metadata lock but touches zero rows —
milliseconds even on a 500M-row table (MySQL >= 8.0.12 for ADD COLUMN). The
lab prints the wall time so you can see it's O(1), not O(rows).
"""
import time

import common as c


def main() -> None:
    cols = c.columns()
    if {"first_name", "last_name"} <= cols:
        c.log("expand: first_name/last_name already present — nothing to do")
        return
    t0 = time.time()
    with c.connect().cursor() as cur:
        cur.execute(f"ALTER TABLE {c.TABLE} "
                    "ADD COLUMN first_name VARCHAR(255) NULL, "
                    "ADD COLUMN last_name VARCHAR(255) NULL, "
                    "ALGORITHM=INSTANT")
        cur.execute(f"SELECT COUNT(*) FROM {c.TABLE}")
        rows = cur.fetchone()[0]
    c.log(f"expand: added first_name/last_name to {rows} rows "
          f"in {(time.time() - t0) * 1000:.0f}ms (ALGORITHM=INSTANT — O(1), not O(rows))")


if __name__ == "__main__":
    main()
