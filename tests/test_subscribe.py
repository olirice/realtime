import pytest
from sqlalchemy import text
from sqlalchemy.engine import Engine
from sqlalchemy.ext.asyncio import AsyncConnection

from realtime.subscribe import replication_slot, subscribe


# @pytest.mark.skipif("GITHUB_SHA" in os.environ, reason="no worky")
@pytest.mark.asyncio
async def test_create_slot(conn: AsyncConnection) -> None:
    async with replication_slot(
        slot_name="test_create_slot", con=conn, drop_on_close=True
    ):
        # TODO assert slot exists
        pass
    # TODO assert slot does not exists


@pytest.mark.asyncio
@pytest.mark.timeout(5)
async def test_subscribe(sync_engine: Engine, conn: AsyncConnection) -> None:

    SLOT_NAME = "test_subscribe_slot"

    sync_engine.execute(text("create table account(id serial primary key);"))
    sync_engine.execute(text("CREATE PUBLICATION realtime_pub_0 FOR ALL TABLES;"))

    # Create and keep replication slot open
    async with replication_slot(slot_name=SLOT_NAME, con=conn, drop_on_close=False):
        pass

    sync_engine.execute(text("insert into account(id) values (1);"))

    ix = 0

    async for message in subscribe(conn, SLOT_NAME, drop_on_close=True):
        print(message)
        ix += 1
        if ix == 3:
            break

    assert ix == 3
