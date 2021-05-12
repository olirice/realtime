from dataclasses import dataclass
from typing import List, Literal, Optional, TypeVar, Union

import regex
from typing_extensions import Protocol

from realtime.exceptions import ParseFailureException

__all__ = ["Message", "TransactionMessage", "CRUDMessage", "parse"]

T = TypeVar("T", bound="Message")


class Message(Protocol):
    command: Literal["BEGIN", "COMMIT", "INSERT", "UPDATE", "DELETE"]


@dataclass
class TransactionMessage(Message):
    """A logical replication message communicating the start or end of a commit

    Examples::

        BEGIN 601
        COMMIT 601
    """

    command: Literal["BEGIN", "COMMIT"]
    lsn: int


@dataclass
class Column:
    column: str
    data_type: str
    value: Optional[str]


@dataclass
class CRUDMessage(Message):
    """A logical replication message communicating an insert, update, or delete

    Example::

        table public.account: INSERT: id[integer]:5 email[text]:'example@example.com' is_email_verified[boolean]:false
        table public.account: UPDATE: id[integer]:5 email[text]:'example@example.com' is_email_verified[boolean]:true
        table public.account: DELETE: id[integer]:5
    """

    command: Literal["INSERT", "UPDATE", "DELETE"]
    schema: Optional[str]
    table: str
    columns: List[Column]


def parse(message: str) -> Union[TransactionMessage, CRUDMessage]:
    from realtime.parse_utils import (
        postprocess,
        preprocess,
        read,
        read_column,
        read_until,
    )

    # First try to parse a transaction message
    # BEGIN 601

    if message[0] in ("B", "C"):
        expression = r"^(BEGIN|COMMIT) (\d+)$"
        match = regex.match(expression, message)

        if not match:
            raise ParseFailureException("Failed to parse message: {}".format(message))

        command = match.captures(1)[0]
        lsn = int(match.captures(2)[0])

        return TransactionMessage(
            command=command,
            lsn=lsn,
        )

    elif message[0] == "t":

        # table schema.table: .....
        remaining = preprocess(message)

        # schema.table: ....
        _, remaining = read_until(remaining, " ")

        # schema.table OR table
        schema_and_table, remaining = read_until(remaining, ": ")

        if "." in schema_and_table:
            schema_maybe_quoted, _, table_maybe_quoted = schema_and_table.partition(".")
            # schema, _ = read_escaped(schema_maybe_quoted)
            # table, _ = read_escaped(table_maybe_quoted)
            schema, _ = read(schema_maybe_quoted)
            table, _ = read(table_maybe_quoted)
        else:
            schema = None
            # table, _ = read_escaped(schema_and_table)
            table, _ = read(schema_and_table)

        assert table is not None

        # *schema* and *table* now set

        # COMMAND: col[type]:value ...
        command, remaining = read_until(remaining, ": ")

        column, remaining = read_column(remaining)
        columns = []
        while column[0] != "":
            columns.append(column)
            column, remaining = read_column(remaining)
            remaining = remaining.strip()

        return CRUDMessage(
            schema=postprocess(schema) if schema else None,
            table=postprocess(table),
            command=postprocess(command),  # type: ignore
            columns=[
                Column(
                    postprocess(col_name),
                    postprocess(type_),
                    postprocess(value) if value is not None else None,
                )
                for col_name, type_, value in columns
            ],
        )

    raise ParseFailureException("Failed to parse message: {}".format(message))
