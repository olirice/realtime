import json
import os
import subprocess
import time
from typing import AsyncGenerator, Generator

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from sqlalchemy.ext.asyncio import (
    AsyncConnection,
    AsyncEngine,
    AsyncSession,
    create_async_engine,
)

TEST_SLOT_NAME = "test_realtime_py"

SYNC_CONN_STR = "postgresql://pytest:pytest_password@localhost:6012/realtime"
CONN_STR = "postgresql+asyncpg://pytest:pytest_password@localhost:6012/realtime"


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
                "supabase/postgres:0.14.0",
            ]
        )
        wait_until_ready()

    # Wait for postgres to become healthy
    wait_until_ready()
    yield
    subprocess.call(["docker", "stop", container_name])


@pytest.fixture(scope="function")
def sync_engine(dockerize_database: None) -> Generator[Engine, None, None]:
    eng = create_engine(SYNC_CONN_STR)
    yield eng


@pytest.fixture(scope="function")
async def engine(dockerize_database: None) -> AsyncGenerator[AsyncEngine, None]:
    eng = create_async_engine(CONN_STR)
    yield eng


@pytest.fixture(scope="function")
async def conn(engine: AsyncEngine) -> AsyncGenerator[AsyncConnection, None]:
    async with engine.connect() as conn_:
        yield conn_
        await conn_.execute(text("rollback"))
