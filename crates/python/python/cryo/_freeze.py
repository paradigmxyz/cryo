from __future__ import annotations

import typing

if typing.TYPE_CHECKING:
    from typing_extensions import Unpack

    from . import _spec


async def async_freeze(
    datatype: str | typing.Sequence[str],
    **kwargs: Unpack[_spec.CryoCliArgs],
) -> None:
    """asynchronously collect data and save to files

    see cryo.parse_kwargs() for descriptions of arguments
    """

    from . import _cryo_rust  # type: ignore
    from . import _args

    if isinstance(datatype, str):
        datatypes = [datatype]
    elif isinstance(datatype, list):
        datatypes = datatype
    else:
        raise Exception('invalid format for datatype(s)')

    cli_args = _args.parse_cli_args(**kwargs)
    return await _cryo_rust._freeze(datatypes, **cli_args)  # type: ignore


def freeze(
    datatype: str | typing.Sequence[str],
    **kwargs: Unpack[_spec.CryoCliArgs],
) -> None:
    """collect data and save to files"""

    import asyncio

    return asyncio.run(async_freeze(datatype, **kwargs))

