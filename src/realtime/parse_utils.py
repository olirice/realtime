from typing import Optional, Tuple
from uuid import uuid4

BRACKET = str(uuid4())
ESC_QUOTE = str(uuid4())

REPLACE_MAP = {"[]": BRACKET, "''": ESC_QUOTE}


def preprocess(text: str) -> str:
    altered = text
    for key, rep in REPLACE_MAP.items():
        altered = altered.replace(key, rep)
    return altered


def postprocess(text: str) -> str:
    altered = text
    for key, rep in REPLACE_MAP.items():
        altered = altered.replace(rep, key)
    return altered


def read_until(text: str, char: str) -> Tuple[str, str]:
    matched, _, remaining = text.partition(char)
    return matched, remaining


def read(text: str) -> Tuple[Optional[str], str]:
    # Reading type
    if text.startswith("["):
        return read_until(text[1:], "]")

    # Reading quoted name or quoted value
    if text.startswith("'"):
        return read_until(text[1:], "'")

    # Reading unquoted name or value
    locs = [x for x in [text.find("["), text.find(" ")] if x > 0]
    if len(locs) == 0:
        return text, ""

    delim = min(locs)
    token, remain = text[:delim], text[delim:]

    if token == "null":
        return None, remain

    return token, remain


def read_column(text: str) -> Tuple[Tuple[str, str, Optional[str]], str]:
    # column_name[type]:value
    name, remain = read(text)
    type_, remain = read(remain)
    remain = remain.strip(":")
    value, remain = read(remain)
    return (name, type_, value), remain.strip(" ")  # type: ignore
