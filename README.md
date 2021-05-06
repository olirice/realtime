# Realtime

<p>

<a href="https://github.com/olirice/realtime/actions"><img src="https://github.com/olirice/realtime/workflows/tests/badge.svg" alt="Tests" height="18"></a>
<a href="https://realtime.readthedocs.io/en/latest/?badge=latest"><img src="https://readthedocs.org/projects/realtime/badge/?version=latest" alt="Tests" height="18"></a>
<a href="https://codecov.io/gh/olirice/realtime"><img src="https://codecov.io/gh/olirice/realtime/branch/master/graph/badge.svg" height="18"></a>
<a href="https://github.com/psf/black">
        <img src="https://img.shields.io/badge/code%20style-black-000000.svg" alt="Codestyle Black" height="18">
    </a>
</p>

<p>
    <a href="https://www.python.org/downloads/"><img src="https://img.shields.io/badge/python-3.8+-blue.svg" alt="Python version" height="18"></a>
  <a href="https://badge.fury.io/py/realtime"><img src="https://badge.fury.io/py/realtime.svg" alt="PyPI version" height="18"></a>
    <a href="https://github.com/olirice/realtime/blob/master/LICENSE"><img src="https://img.shields.io/pypi/l/markdown-subtemplate.svg" alt="License" height="18"></a>
    <a href="https://pypi.org/project/realtime/"><img src="https://img.shields.io/pypi/dm/realtime.svg" alt="Download count" height="18"></a>
</p>

---

**Documentation**: <a href="https://realtime.readthedocs.io/en/latest/" target="_blank">https://realtime.readthedocs.io/en/latest/</a>

**Source Code**: <a href="https://github.com/olirice/realtime" target="_blank">https://github.com/olirice/realtime</a>

---

Realtime enables listening for changes in a PostgreSQL Database

This package is under active development and may change significantly. There is no stability guarentee. If you see something you like, please vendor it, or use at your own risk.

Thanks,

Oli



## Usage

```python
from realtime.subscribe import subscribe
from sqlalchemy.ext.asyncio import create_async_engine

connection_string = "postgresql+asyncpg://<user>:<password>@<host>:<port>/<database>"

engine = create_async_engine(connection_string)

async with engine.connect() as conn:

    subscription = subscribe(con=conn, slot_name="realtime_example")

    async for message in subscription:
        # Business Logic Goes Here
        print(message)
```

where example outputs are:

```python
from realtime.message import TransactionMessage, CRUDMessage, Column

TransactionMessage(command="COMMIT", lsn=95)
# OR
CRUDMessage(
    command="INSERT",
    schema="public",
    table="account",
    columns=[
        Column(column="id", data_type="integer", value=5),
        Column(column="email", data_type="text", value="example@example.com"),
    ]
)
```
