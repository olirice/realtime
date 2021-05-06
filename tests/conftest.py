import json
import os
import subprocess
import time
from typing import AsyncGenerator, Generator

import pytest
import sqlalchemy
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from sqlalchemy.ext.asyncio import (
    AsyncConnection,
    AsyncEngine,
    AsyncSession,
    create_async_engine,
)
from sqlalchemy.orm import sessionmaker

TEST_SLOT_NAME = "test_realtime_py"

SYNC_CONN_STR = "postgresql://pytest:pytest_password@localhost:6012/realtime"
CONN_STR = "postgresql+asyncpg://pytest:pytest_password@localhost:6012/realtime"

LOGICAL_SETUP = [
    text("ALTER SYSTEM SET wal_level = logical;"),
    text("ALTER SYSTEM SET max_replication_slots = 5;"),
]


@pytest.fixture(scope="session")
def dockerize_database() -> Generator[None, None, None]:
    container_name = "realtime_pg"

    def is_ready() -> bool:
        """Check if the database is ready"""
        try:
            out = subprocess.check_output(["docker", "inspect", container_name])
        except:
            return False
        container_info = json.loads(out)
        container_health_status = container_info[0]["State"]["Health"]["Status"]
        if container_health_status == "healthy":
            return True
        return False

    def wait_until_ready() -> None:
        for _ in range(10):
            if is_ready():
                break
            time.sleep(1)
        else:
            raise Exception("Container never became healthy")

    def prep_database() -> None:
        """Set up for logical replication"""
        sync_engine = create_engine(SYNC_CONN_STR).execution_options(
            isolation_level="AUTOCOMMIT"
        )
        conn = sync_engine.connect()
        for command in LOGICAL_SETUP:
            conn.execute(command)
        sync_engine.dispose()
        res = subprocess.check_output(["docker", "restart", "realtime_pg"])

    # Skip container setup if in CI
    if not "GITHUB_SHA" in os.environ and not is_ready():

        subprocess.call(
            [
                "docker",
                "run",
                "--rm",
                "--name",
                container_name,
                "-p",
                "6012:5432",
                "-d",
                "-e",
                "POSTGRES_DB=realtime",
                "-e",
                "POSTGRES_PASSWORD=pytest_password",
                "-e",
                "POSTGRES_USER=pytest",
                "--health-cmd",
                "pg_isready",
                "--health-interval",
                "3s",
                "--health-timeout",
                "3s",
                "--health-retries",
                "10",
                "postgres:13",
            ]
        )
        wait_until_ready()

    # Wait for postgres to become healthy
    prep_database()
    wait_until_ready()
    yield
    subprocess.call(["docker", "stop", "realtime_pg"])


@pytest.fixture(scope="function")
def sync_engine(dockerize_database: None) -> Generator[Engine, None, None]:
    eng = create_engine(SYNC_CONN_STR)
    yield eng
    eng.dispose()


@pytest.fixture(scope="function")
async def engine(dockerize_database: None) -> AsyncGenerator[AsyncEngine, None]:
    eng = create_async_engine(CONN_STR)
    yield eng
    await eng.dispose()


@pytest.fixture(scope="function")
async def conn(engine: AsyncEngine) -> AsyncGenerator[AsyncConnection, None]:
    async with engine.connect() as c:
        yield c


@pytest.fixture(scope="function")
async def sess(engine: AsyncEngine) -> AsyncGenerator[AsyncSession, None]:

    Session = sessionmaker(  # type: ignore
        bind=engine,
        class_=AsyncSession,
        expire_on_commit=False,
    )

    async with engine.begin() as conn:

        # Bind a session to the top level transaction
        _session = Session(bind=conn)

        # Start a savepoint that we can rollback to in the transaction
        _session.begin_nested()

        @sqlalchemy.event.listens_for(_session.sync_session, "after_transaction_end")
        def restart_savepoint(sess, trans):  # type: ignore
            """Register event listener to clean up the sqla objects of a session after a transaction ends"""
            if trans.nested and not trans._parent.nested:
                # Expire all objects registered against the session
                sess.expire_all()
                sess.begin_nested()

            yield _session

        yield _session

        # Close the session object
        await _session.close()

        # Rollback to the savepoint, eliminating everything that happend to the _session
        await conn.rollback()  # type: ignore
