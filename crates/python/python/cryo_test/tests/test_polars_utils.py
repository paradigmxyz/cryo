import polars as pl
import pytest

from cryo_test import perform_comparison
from cryo_test.polars_utils import find_df_differences


@pytest.mark.parametrize('lengths', [(1, 2), (2, 1), (0, 1)])
def test_different_row_counts(lengths):
    dfs = {
        name: pl.DataFrame({'x': list(range(length))}, schema={'x': pl.Int64})
        for name, length in zip(['old', 'new'], lengths)
    }

    assert find_df_differences(dfs) == [
        'old and new have different number of rows'
    ]


def test_different_row_counts_in_sequence():
    dfs = [pl.DataFrame({'x': [1]}), pl.DataFrame({'x': [1, 2]})]

    assert find_df_differences(dfs) == [
        'lhs and rhs have different number of rows'
    ]


def test_different_row_counts_in_lazy_frames():
    dfs = {
        'old': pl.DataFrame({'x': [1]}).lazy(),
        'new': pl.DataFrame({'x': [1, 2]}).lazy(),
    }

    assert find_df_differences(dfs) == [
        'old and new have different number of rows'
    ]


def test_row_count_differences_are_retained_across_comparisons():
    dfs = {
        'old': pl.DataFrame({'x': [1]}),
        'new': pl.DataFrame({'x': [1, 2]}),
        'other': pl.DataFrame({'x': [1, 2, 3]}),
    }

    assert find_df_differences(dfs) == [
        'old and new have different number of rows',
        'old and other have different number of rows',
    ]


def test_equal_frames():
    df = pl.DataFrame({'x': [1, 2]})

    assert find_df_differences({'old': df, 'new': df.clone()}) == []


def test_same_row_count_with_different_values():
    dfs = {
        'old': pl.DataFrame({'x': [1, 2]}),
        'new': pl.DataFrame({'x': [1, 3]}),
    }

    differences = find_df_differences(dfs)

    assert len(differences) == 1
    assert differences[0].startswith(
        'rows of old and new do not match because '
    )


def test_compare_parquet_outputs_reports_row_count_difference(tmp_path, capsys):
    for batch, values in [('old', [1]), ('new', [1, 2])]:
        data_dir = tmp_path / 'data' / batch / 'sample' / 'blocks'
        data_dir.mkdir(parents=True)
        pl.DataFrame({'x': values}).write_parquet(data_dir / 'blocks.parquet')

    perform_comparison(
        batches={
            'old': {'sample': 'cryo blocks'},
            'new': {'sample': 'cryo blocks'},
        },
        comparison_dir=str(tmp_path),
        steps=['compare'],
    )

    output = capsys.readouterr().out
    assert 'old and new have different number of rows' in output
    assert 'Total differences: 1' in output
    assert 'equal' not in output
