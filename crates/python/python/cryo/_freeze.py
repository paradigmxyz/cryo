from __future__ import annotations

import typing

if typing.TYPE_CHECKING:
    from . import _spec


async def async_freeze(
    *args,
    datatype: str,
    start_block: _spec.BlockReference = 0,
    end_block: _spec.BlockReference = 'latest',
    hex: bool = False,
    file_format: _spec.FileFormat = 'parquet',
    compresion: _spec.FileCompression = 'lz4',
    **kwargs,
) -> None:
    """asynchronously collect data and save to files"""

    from . import _cryo_rust

    if isinstance(datatype, str):
        datatypes = [datatype]
    elif isinstance(datatype, list):
        datatypes = datatype
    else:
        raise Exception('invalid format for datatype(s)')

    if start_block is not None and end_block is not None:
        blocks = str(start_block) + ':' + str(end_block)
    elif start_block is not None:
        blocks = str(start_block) + ':'
    elif end_block is not None:
        blocks = ':' + str(end_block)
    else:
        blocks = None
    kwargs['blocks'] = [blocks]

    if file_format == 'parquet':
        pass
    elif file_format == 'json':
        kwargs['json'] = True
    elif file_format == 'csv':
        kwargs['csv'] = True
    elif file_format == 'avro':
        kwargs['avro'] = True
    else:
        raise Exception('unknown file_format')

    return await _cryo_rust._freeze(
        *args,
        datatype=datatypes,
        **kwargs,
    )


def freeze(*args, **kwargs) -> None:
    """collect data and save to files"""

    import asyncio

    return asyncio.run(async_freeze(*args, **kwargs))

