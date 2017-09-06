import asyncio
import sqlalchemy as sa

from aiomysql.sa import create_engine

metadata = sa.MetaData()

tbl = sa.Table('0', metadata,
               sa.Column('E', sa.Float),
               sa.Column('N', sa.Float),
               sa.Column('O', sa.Float))


async def go(loop):
    engine = await create_engine(user='root', db='test_tables',
                                 host='127.0.0.1', password='coqueta', loop=loop)
    async with engine.acquire() as conn:
        await conn.execute(tbl.update().values(E=0.66))
        async for row in conn.execute(tbl.select()):
            print(row.E, row.N, row.O)

    engine.close()
    await engine.wait_closed()


loop = asyncio.get_event_loop()
loop.run_until_complete(go(loop))
