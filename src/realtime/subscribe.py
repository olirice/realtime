import asyncio
from contextlib import asynccontextmanager
from typing import AsyncGenerator

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncConnection

from realtime.message import Message, parse


async def subscribe(
    con: AsyncConnection,
    slot_name: str = "realtime_py",
    poll_delay: float = 0.1,
    drop_on_close: bool = True,
) -> AsyncGenerator[Message, None]:
    """Subscribe to a PostgreSQL +9.4 database for changes"""

    GET_UPDATES = text(
        "SELECT lsn, xid, data from pg_logical_slot_get_changes(:slot_name, NULL, NULL)"
    )

    async with replication_slot(
        slot_name=slot_name, con=con, drop_on_close=drop_on_close
    ):

        while True:
            cursor = await con.execute(GET_UPDATES, {"slot_name": slot_name})

            for _, _, data in cursor:
                yield parse(data)

            # Sleep for 0.25 seconds before polling again
            await asyncio.sleep(poll_delay)


@asynccontextmanager
async def replication_slot(
    slot_name: str, con: AsyncConnection, drop_on_close: bool
) -> AsyncGenerator[None, None]:

    CREATE_SLOT = text(
        """
    with check_exists as (
        SELECT
            count(1) = 1 as already_exists
        FROM
            pg_replication_slots
        WHERE
            slot_name = :slot_name
    )

    SELECT
        CASE
            WHEN already_exists THEN 0
            ELSE (
                SELECT
                    1
                FROM
                    pg_create_logical_replication_slot(
                        :slot_name,
                        'test_decoding'
                    )
            )
        END as res
    FROM
        check_exists
    """
        ""
    )
    DROP_SLOT = text("SELECT pg_drop_replication_slot(:slot_name);")

    params = dict(slot_name=slot_name)

    await con.execute(CREATE_SLOT, params)
    try:
        yield
    finally:
        if drop_on_close:
            await con.execute(DROP_SLOT, params)
