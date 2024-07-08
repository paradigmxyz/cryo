from __future__ import annotations

import os
import typing

import polars as pl

from . import _filesystem
from . import _spec

if typing.TYPE_CHECKING:
    from typing_extensions import Unpack


def get_glob(
    datatype: str,
    *,
    network: str | int,
    label: str | None = None,
    cryo_root: str | None = None,
    data_dir: str | None = None,
    start_block: int | None = None,
    end_block: int | None = None,
    filetype: str = 'parquet',
) -> str:
    import mesc

    # get network name
    if isinstance(network, str):
        network_name = network
    else:
        network_name_option = mesc.get_network_name(network)
        if network_name_option is None:
            raise Exception('network name not found')
        else:
            network_name = network_name_option

    # create filename glob
    if label is None:
        file_template = '{network}__{datatype}__*.{filetype}'
    else:
        file_template = '{network}__{datatype}__{label}__*.{filetype}'
    file_glob = file_template.format(
        network=network_name,
        datatype=datatype,
        filetype=filetype,
    )

    # get data directory
    if data_dir is None:
        if cryo_root is not None:
            data_dir = os.path.join(cryo_root, network_name)
        else:
            raise Exception('must specify cryo_root or data_dir')

    # TODO: tune glob string to block range

    return os.path.join(data_dir, file_glob)


def get_file_list(
    datatype: str,
    *,
    network: str | int,
    label: str | None = None,
    cryo_root: str | None = None,
    data_dir: str | None = None,
    start_block: int | None = None,
    end_block: int | None = None,
    filetype: str = 'parquet',
) -> list[str]:
    # TODO: get more specific
    import glob

    path_glob = get_glob(
        datatype=datatype,
        network=network,
        cryo_root=cryo_root,
        data_dir=data_dir,
        start_block=start_block,
        end_block=end_block,
        filetype=filetype,
    )
    return glob.glob(path_glob)


def scan(
    datatype: str,
    *,
    network: str | int,
    cryo_root: str | None = None,
    data_dir: str | None = None,
    start_block: int | None = None,
    end_block: int | None = None,
    filetype: str = 'parquet',
    **kwargs: Unpack[_spec.CryoCliArgs],
) -> pl.LazyFrame:
    if len(kwargs) > 0:
        raise Exception('args not accepted for scan(): ' + str(list(kwargs.keys())))
    files = _filesystem.get_file_list(
        datatype=datatype,
        network=network,
        cryo_root=cryo_root,
        data_dir=data_dir,
        start_block=start_block,
        end_block=end_block,
        filetype=filetype,
    )
    return pl.scan_parquet(files)


def is_data_saved(datatype: str, **kwargs: Unpack[_spec.CryoCliArgs]) -> bool:
    raise NotImplementedError()
