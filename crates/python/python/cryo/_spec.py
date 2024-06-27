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
        contract: typing.Sequence[str | bytes | None]
        topic0: typing.Sequence[str | bytes | None]
        topic1: typing.Sequence[str | bytes | None]
        topic2: typing.Sequence[str | bytes | None]
        topic3: typing.Sequence[str | bytes | None]
        inner_request_size: int | None
        no_verbose: bool

        timestamps: typing.Sequence[str] | None
        txs: typing.Sequence[str] | None
        u256_types: typing.Sequence[str] | None
        exclude_failed: bool
        chunk_order: str | None
        max_retries: int
        initial_backoff: int
        partition_by: typing.Sequence[str] | None
        subdirs: typing.Sequence[str]
        label: str | None
        report_dir: str | None
        no_report: bool
        address: typing.Sequence[str] | None
        to_address: typing.Sequence[str] | None
        from_address: typing.Sequence[str] | None
        call_data: typing.Sequence[str] | None
        function: typing.Sequence[str] | None
        inputs: typing.Sequence[str] | None
        slot: typing.Sequence[str] | None
        js_tracer: str | None
        verbose: bool
        event_signature: str | None
