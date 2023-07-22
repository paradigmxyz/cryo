from __future__ import annotations
import typing

if typing.TYPE_CHECKING:
    import polars as pl


async def async_collect(
    *args, output_format='polars', **kwargs
) -> pl.DataFrame:
    """asynchronously collect data and return as dataframe"""

    from . import _cryo_rust

    result = await _cryo_rust._collect(*args, **kwargs)

    if output_format == 'polars':
        return result
    elif output_format == 'pandas':
        return result.to_pandas()
    elif output_format == 'list_of_dict':
        return result.to_dicts()
    elif output_format == 'dict_of_list':
        return result.to_dict(as_series=False)
    else:
        raise Exception('unknown output format')


def collect(*args, **kwargs) -> pl.DataFrame:
    """collect data and return as dataframe"""

    import asyncio

    return asyncio.run(async_collect(*args, **kwargs))

