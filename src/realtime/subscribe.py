from typing import Union, AsyncGenerator
from realtime.messages import parse, Message, TransactionMessage
import asyncio
from contextlib import asynccontextmanager
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine, AsyncSession
from sqlalchemy import text, select, literal, literal_column

Connection = Union[AsyncSession, AsyncEngine, AsyncConnection]


async def subscribe(
    con: Connection, slot_name: str = "realtime_py", poll_delay: int = 0.1
) -> AsyncGenerator[Message, None]:
    """Subscribe to a PostgreSQL +9.4 database for changes"""

    GET_UPDATES = text(
        "SELECT lsn, xid, data from pg_logical_slot_get_changes(:slot_name, NULL, NULL)"
    )

    async with replication_slot(slot_name=slot_name, con=con):

        cursor = await con.execute(GET_UPDATES, {"slot_name": slot_name})

        for _, _, data in cursor:
            yield parse(data)

        # Sleep for 0.25 seconds before polling again
        await asyncio.sleep(poll_delay)


@asynccontextmanager
async def replication_slot(
    slot_name: str, con: Connection
) -> AsyncGenerator[None, None]:

    CHECK_SLOT = text(
        "SELECT count(1) FROM pg_replication_slots where slot_name = :slot_name"
    )
    CREATE_SLOT = text(
        "SELECT * FROM pg_create_logical_replication_slot(:slot_name, 'test_decoding');"
    )
    DROP_SLOT = text("SELECT pg_drop_replication_slot(:slot_name);")

    params = dict(slot_name=slot_name)

    ((exists,),) = await con.execute(CHECK_SLOT, params)

    if not exists:
        await con.execute(CREATE_SLOT, params)

    yield

    await con.execute(DROP_SLOT, params)
