import asyncio
import asyncpg
from config import DB_SETTINGS, DB_POOL_SETTINGS


class DBPool(object):
    def __init__(self):
        self._pool = None

    @classmethod
    async def create(cls, cnf):
        self = cls()
        self._pool = await asyncpg.create_pool(**cnf, **DB_POOL_SETTINGS)
        return self

    async def _exec(self, method, query, *args):
        async with self._pool.acquire() as conn:
            async with conn.transaction():
                return await getattr(conn, method)(query, *args)

    async def fetch(self, query, *args):
        return await self._exec('fetch', query, *args)

    async def fetchval(self, query, *args):
        return await self._exec('fetchval', query, *args)

    async def fetchrow(self, query, *args):
        return await self._exec('fetchrow', query, *args)

    async def execute(self, query, *args):
        return await self._exec('execute', query, *args)


loop = asyncio.get_event_loop()
db = loop.run_until_complete(DBPool.create(DB_SETTINGS))
