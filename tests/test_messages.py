import pytest

from realtime.message import Column, CRUDMessage, TransactionMessage, parse


@pytest.mark.parametrize(
    "message,result",
    [
        ("BEGIN 501", TransactionMessage(command="BEGIN", lsn=501)),
        ("COMMIT 95", TransactionMessage(command="COMMIT", lsn=95)),
    ],
)
def test_parse_transaction(message: str, result: TransactionMessage) -> None:
    assert parse(message) == result


def test_parse_crud() -> None:
    message = "table public.account: INSERT: id[integer]:5 email[text]:'e@e.c' is_e_vd[boolean]:false"
    assert parse(message) == CRUDMessage(
        command="INSERT",
        schema="public",
        table="account",
        columns=[
            Column(column="id", data_type="integer", value="5"),
            Column(column="email", data_type="text", value="e@e.c"),
            Column(column="is_e_vd", data_type="boolean", value="false"),
        ],
    )


def test_parse_crud_hard() -> None:
    message = "table public.account: INSERT: id[integer]:5 email[text]:'e@e.c' is_e_vd[boolean]:false xxx[timestamp without time zone]:'2021 012 11' somearr[integer[]]:'{4,3}'"
    assert parse(message) == CRUDMessage(
        command="INSERT",
        schema="public",
        table="account",
        columns=[
            Column(column="id", data_type="integer", value="5"),
            Column(column="email", data_type="text", value="e@e.c"),
            Column(column="is_e_vd", data_type="boolean", value="false"),
            Column(
                column="xxx",
                data_type="timestamp without time zone",
                value="2021 012 11",
            ),
            Column(column="somearr", data_type="integer[]", value="{4,3}"),
        ],
    )
