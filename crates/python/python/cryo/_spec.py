import typing

if typing.TYPE_CHECKING:
    from typing import Literal
    from typing import Union

    BlockReference = Union[int | Literal['latest']]
    FileCompression: Union[Literal['lz4']]
    FileFormat = Union[
        Literal['parquet'],
        Literal['csv'],
        Literal['json'],
        Literal['avro'],
    ]

