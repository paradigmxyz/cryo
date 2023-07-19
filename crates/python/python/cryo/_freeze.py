from __future__ import annotations


async def async_freeze(
    datatype: str,
    *args,
    **kwargs,
) -> None:
    """asynchronously collect data and save to files"""

    from . import _cryo_rust

    if isinstance(datatype, str):
        datatypes = [datatype]
    elif isinstance(datatypes, list):
        datatypes = datatype
    else:
        raise Exception('invalid format for datatype(s)')

    return await _cryo_rust._freeze(
        datatype=datatypes,
        *args,
        **kwargs,
    )


def freeze(*args, **kwargs) -> None:
    """collect data and save to files"""

    import asyncio

    return asyncio.run(async_freeze(*args, **kwargs))

