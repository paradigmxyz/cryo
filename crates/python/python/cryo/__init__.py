from .cryo import *

__doc__ = cryo.__doc__
if hasattr(cryo, "__all__"):
    __all__ = cryo.__all__


async def freeze(*args, **kwargs):
    return await _freeze(*args, **kwargs)

