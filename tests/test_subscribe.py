import os

import pytest
from sqlalchemy import text
from sqlalchemy.engine import Engine
from sqlalchemy.ext.asyncio import AsyncConnection

from realtime.subscribe import replication_slot, subscribe


@pytest.mark.skipif("GITHUB_SHA" in os.environ)
@pytest.mark.asyncio
async def test_create_slot(conn: AsyncConnection) -> None:
    async with replication_slot(slot_name="test_create_slot", con=conn):
        # TODO assert slot exists
        pass

    # TODO assert slot does not exists


@pytest.mark.skip(reason="hangs forever")
@pytest.mark.asyncio
async def test_subscribe(sync_engine: Engine, conn: AsyncConnection) -> None:

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
