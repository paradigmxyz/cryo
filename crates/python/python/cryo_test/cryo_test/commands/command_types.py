from __future__ import annotations

import typing


class PartialCommandArgs(typing.TypedDict, total=False):
    datatype: str | tuple[str, ...]
    time_dimension: str
    data_root: str
    executable: str
    extra_args: list[str]
    name: str | None
    interface: typing.Literal['cli', 'python']
    rpc: str


class CommandArgs(typing.TypedDict):
    datatype: str | tuple[str, ...]
    time_dimension: str
    data_root: str
    executable: str
    extra_args: list[str]
    name: str | None
    interface: typing.Literal['cli', 'python']
    rpc: str
