from __future__ import annotations

import typing

from . import _spec

if typing.TYPE_CHECKING:
    from typing_extensions import Unpack


def parse_cli_args(
    start_block: _spec.BlockReference | None = None,
    end_block: _spec.BlockReference | None = None,
    file_format: _spec.FileFormat = 'parquet',
    **kwargs: Unpack[_spec.CryoCliArgs],
) -> _spec.CryoCliArgs:
    """
    TODO: write this part in rust
    """

    if start_block is not None and end_block is not None:
        kwargs['blocks'] = [str(start_block) + ':' + str(end_block)]
    elif start_block is not None:
        kwargs['blocks'] = [str(start_block) + ':']
    elif end_block is not None:
        kwargs['blocks'] = [':' + str(end_block)]

    if file_format == 'parquet':
        pass
    elif file_format == 'json':
        kwargs['json'] = True
    elif file_format == 'csv':
        kwargs['csv'] = True
    # elif file_format == 'avro':
    #     kwargs['avro'] = True
    else:
        raise Exception('unknown file_format')

    return kwargs

