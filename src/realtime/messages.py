from dataclasses import dataclass
from typing_extensions import Annotated, Protocol
from typing import Any, Callable, Type, List, Literal, TypeVar
from functools import lru_cache
from abc import abstractmethod
import regex

from realtime.exceptions import ParseFailureException

__all__ = ["Message", "TransactionMessage", "CRUDMessage", "parse"]

T = TypeVar("T", bound="Message")


class Message(Protocol):
    command: Literal["BEGIN", "COMMIT", "INSERT", "UPDATE", "DELETE"]

    @classmethod
    @abstractmethod
    def parse(cls: Type[T], message: str) -> T:
        raise NotImplementedError()


@dataclass
class TransactionMessage(Message):
    """A logical replication message communicating the start or end of a commit

    Examples::

        BEGIN 601
        COMMIT 601
    """

    command: Literal["BEGIN", "COMMIT"]
    lsn: int

    @classmethod
    def parse(cls: Type["TransactionMessage"], message: str) -> "TransactionMessage":
        """initialize a TransactionMessage instance from a logical replication string message"""
        expression = r"^(BEGIN|COMMIT) (\d+)$"
        match = regex.match(expression, message)

        if not match:
            raise ParseFailureException("Failed to parse message: {}".format(message))

        command = match.captures(1)[0]
        lsn = match.captures(2)[0]

        return cls(
            command=command,
            lsn=lsn,
        )


@dataclass
class Column:
    column: str
    data_type: str
    value: str


@dataclass
class CRUDMessage(Message):
    """A logical replication message communicating an insert, update, or delete

    Example::

        table public.account: INSERT: id[integer]:5 email[text]:'example@example.com' is_email_verified[boolean]:false
        table public.account: UPDATE: id[integer]:5 email[text]:'example@example.com' is_email_verified[boolean]:true
        table public.account: DELETE: id[integer]:5
    """

    command: Literal["INSERT", "UPDATE", "DELETE"]
    schema: str
    table: str
    columns: List[Column]

    @classmethod
    def parse(cls: Type["CRUDMessage"], message: str) -> "CRUDMessage":
        """initialize a CRUDMessage instance from a logical replication string message"""
        expression = r"""table (?P<schema>[\w"]*).(?P<table_name>[\w"]*): (?P<command>UPDATE|INSERT|DELETE):( ([\w"]*)\[([\w"]*)\]:(.+?))*$"""
        match = regex.match(expression, message)

        if not match:
            raise ParseFailureException("Failed to parse message: {}".format(message))

        command = match.captures(3)[0]
        schema = match.captures(1)[0]
        table = match.captures(2)[0]

        columns = [
            Column(column=column, data_type=data_type, value=value)
            for column, data_type, value in zip(
                match.captures(5), match.captures(6), match.captures(7)
            )
        ]

        return cls(command=command, schema=schema, table=table, columns=columns)


def parse(msg: str) -> Message:
    """Parses a `output_slot=test_decoding` logical replication message from Postgres 9.4+"""

    # CRUDMessage always begins with "table ..."
    if msg[0] == "t":
        return CRUDMessage.parse(msg)
    # TransactionMessage always begins with "BEGIN ..." or "COMMIT ..."
    elif msg[0] in ("B", "C"):
        return TransactionMessage.parse(msg)

    raise ParseFailureException("Failed to parse message: {}".format(msg))
