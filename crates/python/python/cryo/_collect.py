from __future__ import annotations
import typing

if typing.TYPE_CHECKING:
    from typing_extensions import Unpack
    from typing import Any

    import pandas as pd
    import polars as pl
    from . import _spec

    ListOfDicts = list[dict[str, Any]]
    DictOfLists = dict[str, list[Any]]


async def async_collect(
    datatype: _spec.Datatype,
    output_format: _spec.PythonOutput = 'polars',
    **kwargs: Unpack[_spec.CryoCliArgs],
) -> pl.DataFrame | pd.DataFrame | ListOfDicts | DictOfLists:
    """asynchronously collect data and return as dataframe"""

    from . import _args
    from . import _cryo_rust  # type: ignore

    cli_args = _args.parse_cli_args(**kwargs)
    result: pl.DataFrame = await _cryo_rust._collect(datatype, **cli_args)

    if output_format == 'polars':
        return result
    elif output_format == 'pandas':
        return result.to_pandas()
    elif output_format == 'list':
        return result.to_dicts()
    elif output_format == 'dict':
        return result.to_dict(as_series=False)
    else:
        raise Exception('unknown output format')


def collect(
    datatype: _spec.Datatype,
    output_format: _spec.PythonOutput = 'polars',
    **kwargs: Unpack[_spec.CryoCliArgs],
) -> pl.DataFrame | pd.DataFrame | ListOfDicts | DictOfLists:
    """collect data and return as dataframe"""

    import asyncio

    return asyncio.run(
        async_collect(datatype, output_format=output_format, **kwargs)
    )

