from __future__ import annotations

import typing

from . import _spec

if typing.TYPE_CHECKING:
    from typing_extensions import Unpack


def parse_cli_args(
    start_block: _spec.BlockReference | None = None,
    end_block: _spec.BlockReference | None = None,
    file_format: _spec.FileFormat = 'parquet',
    verbose: bool = True,
    **kwargs: Unpack[_spec.CryoCliArgs],
) -> _spec.CryoCliArgs:
    """
    TODO: write this part in rust
    """

    kwargs['blocks'] = [get_blocks_str(start_block, end_block)]

    if file_format == 'parquet':
        pass
    elif file_format == 'json':
        kwargs['json'] = True
    elif file_format == 'csv':
        kwargs['csv'] = True
    else:
        raise Exception('unknown file_format')

    kwargs['no_verbose'] = not verbose

    return kwargs


def get_blocks_str(
    start_block: int | str | None,
    end_block: int | str | None,
) -> str:
    if start_block is not None and end_block is not None:
        return str(start_block) + ':' + str(end_block)
    elif start_block is not None:
        return str(start_block) + ':'
    elif end_block is not None:
        return ':' + str(end_block)
    else:
        return ':'


def parse_blocks_str(blocks_str: str) -> tuple[int, int]:
    start_block, end_block = blocks_str.split(':')
    return int(start_block), int(end_block)
