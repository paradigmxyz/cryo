from __future__ import annotations

import typing

if typing.TYPE_CHECKING:
    from typing import Mapping, Sequence
    import polars as pl

    DataRef = typing.Union[str, list[str], pl.DataFrame, pl.LazyFrame]


def find_df_differences(
    dfs: Sequence[DataRef] | Mapping[str, DataRef],
) -> list[str]:
    """input is list or dict of 1. glob, 2. DataFrame, or 3. LazyFrame"""
    differences = find_schema_differences(dfs)
    if len(differences) > 0:
        differences.append('(omitting row check because schemas not equal)')
    else:
        differences += find_row_differences(dfs)
    return differences


def _dfs_to_map(
    dfs: Sequence[DataRef] | Mapping[str, DataRef],
) -> Mapping[str, pl.DataFrame]:
    if isinstance(dfs, dict):
        return {key: _resolve(value) for key, value in dfs.items()}
    elif isinstance(dfs, list):
        if len(dfs) == 2:
            return {'lhs': _resolve(dfs[0]), 'rhs': _resolve(dfs[1])}
        elif len(dfs) > 2:
            return {'input_' + str(i): _resolve(df) for i, df in enumerate(dfs)}
        else:
            return {'only': _resolve(df) for df in dfs}
    else:
        raise Exception()


def _resolve(ref: DataRef) -> pl.DataFrame:
    import polars as pl

    if isinstance(ref, str):
        return pl.read_parquet(ref)
    elif isinstance(ref, list) and all(isinstance(item, str) for item in ref):
        return pl.read_parquet(ref)
    elif isinstance(ref, pl.DataFrame):
        return ref
    elif isinstance(ref, pl.LazyFrame):
        return ref.collect()
    else:
        raise Exception('cannot resolve DataFrame from type')


def find_schema_differences(
    dfs: Sequence[DataRef] | Mapping[str, DataRef],
) -> list[str]:
    # package inputs
    data = _dfs_to_map(dfs)
    if len(data) <= 1:
        return []
    data_list = list(data.items())

    # compare all pairs
    differences = []
    name, df = data_list[0]
    for other_name, other_df in data_list[1:]:
        # check missing columns
        missing_columns = []
        for column in other_df.schema:
            if column not in df.schema:
                missing_columns.append(column)
        if len(missing_columns) > 0:
            difference = (
                other_name
                + ' has extra columns compared to '
                + name
                + ': '
                + ', '.join(missing_columns)
            )
            differences.append(difference)
            continue

        # check extra columns
        extra_columns = []
        for column in df.schema:
            if column not in other_df.schema:
                extra_columns.append(column)
        if len(extra_columns) > 0:
            difference = (
                name
                + ' has extra columns compared to '
                + other_name
                + ': '
                + ', '.join(extra_columns)
            )
            differences.append(difference)
            continue

        # check column types
        different_types = {}
        for column in df.schema.keys():
            if df.schema[column] != other_df.schema[column]:
                different_types[column] = {
                    name: df.schema[column],
                    other_name: other_df.schema[column],
                }
        if len(different_types) > 0:
            difference = (
                name
                + ' and '
                + other_name
                + ' have different column types: '
                + str(different_types)
            )
            differences.append(difference)
            continue

        # check column order
        if list(df.schema.keys()) != list(other_df.schema.keys()):
            difference = (
                'columns of '
                + name
                + ' and '
                + other_name
                + ' are in different order'
            )
            differences.append(difference)
            continue

    return differences


def find_row_differences(
    dfs: Sequence[DataRef] | Mapping[str, DataRef],
) -> list[str]:
    import polars.testing

    # package inputs
    data = _dfs_to_map(dfs)
    if len(data) <= 1:
        return []
    data_list = list(data.items())

    # compare all pairs
    differences = []
    name, df = data_list[0]
    for other_name, other_df in data_list[1:]:
        # row count
        if len(df) != len(other_df):
            difference = (
                name + ' and ' + other_name + ' have different number of rows'
            )
            continue

        # row values
        try:
            polars.testing.assert_frame_equal(df, other_df)
        except AssertionError as e:
            difference = (
                'rows of '
                + name
                + ' and '
                + other_name
                + ' do not match because '
                + e.args[0]
            )
            differences.append(difference)
            continue

    return differences
