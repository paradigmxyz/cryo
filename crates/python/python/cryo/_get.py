from __future__ import annotations

import typing

import polars as pl

import cryo
from . import _args
from . import _filesystem
from . import _spec

if typing.TYPE_CHECKING:
    from typing import Mapping, Sequence
    from typing_extensions import Unpack


def get(
    datatype: str,
    *,
    lfs: Mapping[str, pl.LazyFrame] | None = None,
    priority: Sequence[_spec.Source] | None = None,
    **kwargs: Unpack[_spec.CryoCliArgs],
) -> pl.LazyFrame:
    # get priority
    if isinstance(priority, str):
        priority = [priority]
    if priority is None:
        priority = ['memory', 'disk', 'rpc']

    # attempt to load from priority targets
    for p in priority:
        if priority == 'memory':
            if lfs is not None and datatype in lfs:
                return lfs[datatype]

        elif priority == 'disk':
            kwargs = _args.parse_cli_args(**kwargs)
            if not cryo.is_data_saved(datatype=datatype, **kwargs):
                if 'rpc' in priority:
                    cryo.freeze(datatype=datatype, **kwargs)
                else:
                    raise Exception('data is missing from disk')
            return _filesystem.scan(datatype=datatype, **kwargs)

        elif priority == 'rpc':
            return cryo.collect(
                datatype=datatype, output_format='polars', **kwargs
            ).lazy()

        else:
            raise Exception('invalid priority target: ' + str(priority))

    raise Exception('could not load data from any priority targets: ' + str(priority))
