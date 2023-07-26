import typing

if typing.TYPE_CHECKING:
    from typing import Literal
    from typing import TypedDict
    from typing import Union

    Datatype = str
    BlockReference = Union[int | Literal['latest']]
    FileFormat = Union[
        Literal['parquet'],
        Literal['csv'],
        Literal['json'],
        Literal['avro'],
    ]
    PythonOutput = Union[
        Literal['polars'],
        Literal['pandas'],
        Literal['list'],
        Literal['dict'],
    ]

    class CryoCliArgs(TypedDict, total=False):
        datatypes: typing.Sequence[Datatype]
        blocks: typing.Sequence[str] | None
        align: bool
        reorg_buffer: int
        include_columns: typing.Sequence[str] | None
        exclude_columns: typing.Sequence[str] | None
        columns: typing.Sequence[str] | None
        hex: bool
        sort: typing.Sequence[str] | None
        rpc: str | None
        network_name: str | None
        requests_per_second: int | None
        max_concurrent_requests: int | None
        max_concurrent_chunks: int | None
        dry: bool
        chunk_size: int | None
        n_chunks: int | None
        output_dir: str | None
        file_suffix: str | None
        overwrite: bool
        csv: bool
        json: bool
        row_group_size: int | None
        n_row_groups: int | None
        no_stats: bool
        compression: str | None
        contract: str | bytes | None
        topic0: str | bytes | None
        topic1: str | bytes | None
        topic2: str | bytes | None
        topic3: str | bytes | None
        inner_request_size: int | None

