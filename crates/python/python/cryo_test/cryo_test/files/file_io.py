from __future__ import annotations

import typing

from . import file_paths

if typing.TYPE_CHECKING:
    import polars as pl

    T = typing.TypeVar('T')


def save_commands(
    comparison_dir: str, batches: dict[str, dict[str, str]]
) -> None:
    import json
    import os

    os.makedirs(comparison_dir, exist_ok=True)
    with open(file_paths.get_commands_path(comparison_dir), 'w') as f:
        json.dump(batches, f)


def load_commands(comparison_dir: str) -> dict[str, dict[str, str]]:
    import json

    with open(file_paths.get_commands_path(comparison_dir)) as f:
        result: dict[str, dict[str, str]] = json.load(f)
        return result


def load_all_data(comparison_dir: str) -> dict[str, dict[str, pl.DataFrame]]:
    import polars as pl

    return _io_data(comparison_dir, pl.read_parquet)


def scan_all_data(comparison_dir: str) -> dict[str, dict[str, pl.LazyFrame]]:
    import polars as pl

    return _io_data(comparison_dir, pl.scan_parquet)


def _io_data(
    comparison_dir: str, f: typing.Callable[[str], T]
) -> dict[str, dict[str, T]]:
    batches = load_commands(comparison_dir)
    data: dict[str, dict[str, T]] = {}
    for batch_name in batches.keys():
        data[batch_name] = {}
        for command in batches[batch_name].keys():
            data[batch_name][command] = f(comparison_dir)
    return data
