"""The legacy reservation store: a Redis counter behind an atomic Lua script.

This is the system being migrated AWAY from. Note it is CORRECT — the Lua
script is atomic, it never oversells (DECR with a floor). Shopify's motive
for leaving Redis wasn't correctness: the reservation state was a bare
counter, unjoinable and unauditable next to the rest of the relational data.
The lab keeps the legacy store correct on purpose so the migration drills
can compare stores honestly [A2].

The script also SADDs the reservation_id, giving verify.py an exactly-once
set to join against.
"""

import common

RESERVE_LUA = """
local remaining = tonumber(redis.call('GET', KEYS[1]) or '0')
if remaining <= 0 then
  return 0
end
redis.call('DECR', KEYS[1])
redis.call('SADD', KEYS[2], ARGV[1])
return 1
"""


class LegacyStore:
    def __init__(self) -> None:
        self.r = common.redis_client()
        self._reserve = self.r.register_script(RESERVE_LUA)

    def reserve(self, reservation_id: str) -> dict:
        ok = self._reserve(
            keys=[common.REDIS_KEY, common.REDIS_KEY + ":acks"],
            args=[reservation_id],
        )
        return {
            "ok": bool(ok),
            "reason": "reserved" if ok else "sold_out",
            "retries": 0,
        }

    def remaining(self) -> int:
        return int(self.r.get(common.REDIS_KEY) or 0)

    def acked_ids(self) -> set[str]:
        return set(self.r.smembers(common.REDIS_KEY + ":acks"))


def compare_stores(conn, store: LegacyStore) -> tuple[set[str], set[str]]:
    """The shadow-mode comparator: which reservation_ids does one store have
    that the other doesn't? Returns (only_in_redis, only_in_mysql)."""
    redis_ids = store.acked_ids()
    with conn.cursor() as cur:
        cur.execute(
            "SELECT reservation_id FROM reservations "
            "WHERE item_id = %s AND state IN ('active','committed')",
            (common.ITEM_ID,),
        )
        mysql_ids = {row[0] for row in cur.fetchall()}
    return redis_ids - mysql_ids, mysql_ids - redis_ids
