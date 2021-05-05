import pytest
from realtime.subscribe import subscribe, replication_slot
from sqlalchemy import text
from sqlalchemy.engine import Engine
from sqlalchemy.ext.asyncio import AsyncSession, AsyncConnection, AsyncEngine


@pytest.mark.asyncio
async def test_create_slot(sess: AsyncSession):
    async with replication_slot(slot_name="test_create_slot", con=sess):
        # TODO assert slot exists
        pass

    # TODO assert slot does not exists


@pytest.mark.asyncio
async def test_subscribe(sync_engine: Engine, conn: AsyncEngine):

    SLOT_NAME = "test_subscribe_slot"

    sync_engine.execute(text("create table account(id serial primary key);"))
    sync_engine.execute(text("CREATE PUBLICATION realtime_pub_0 FOR ALL TABLES;"))
    sync_engine.execute(
        text(
            "select * FROM pg_create_logical_replication_slot(:slot_name, 'test_decoding')"
        ),
        {"slot_name": SLOT_NAME},
    )

    ix = 0

    async for message in subscribe(conn, "test_realtime_py"):
        pass

    assert ix > 0
