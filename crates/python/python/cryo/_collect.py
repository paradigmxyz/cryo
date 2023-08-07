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

    # parse inputs
    cli_args = _args.parse_cli_args(**kwargs)

    # fix chunk size
    cli_args['chunk_size'] = 20_000_000

    # collect data
    result: pl.DataFrame = await _cryo_rust._collect(datatype, **cli_args)

    # format output
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

    coroutine = async_collect(datatype, output_format=output_format, **kwargs)

    try:
        import concurrent.futures
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        with concurrent.futures.ThreadPoolExecutor() as executor:
            future = executor.submit(loop.run_until_complete, coroutine)  # type: ignore
            return future.result()  # type: ignore
    except RuntimeError:
        return asyncio.run(coroutine)

