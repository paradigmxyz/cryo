import tempfile

import pytest

import cryo
import polars as pl


queries = [
    {
        'datatype': ['blocks'],
        'start_block': 17000000,
        'end_block': 17000100,
    }
]

file_formats = [
    ['parquet', pl.read_parquet],
    ['csv', pl.read_csv],
    # ['avro', pl.read_avro],
    ['json', pl.read_json],
]


@pytest.mark.parametrize('query', queries)
@pytest.mark.parametrize('format', file_formats)
def test_file_output(query, format):
    extension, reader = format
    output_dir = tempfile.mkdtemp()
    if extension != 'parquet':
        query = dict(query, **{extension: True})
    result = cryo.freeze(output_dir=output_dir, **query)
    for datatype in query['datatype']:
        path = result['paths_by_type'][datatype]
        assert isinstance(path, list) and len(path) == 1
        path = path[0]
        df_freeze = reader(path)
        query_without_datatype = dict(query)
        del query_without_datatype['datatype']
        df_collect = cryo.collect(datatype, **query_without_datatype)
        assert df_freeze.frame_equal(df_collect)


python_formats = [
    'polars',
    'pandas',
    'list',
    'dict',
]


@pytest.mark.parametrize('query', queries)
@pytest.mark.parametrize('format', python_formats)
def python_output_python_formats(query, format):
    output_format, output_type = format
    df = cryo.collect(output_format=output_format, **query)
    assert isinstance(df, output_type)

