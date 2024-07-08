import tempfile

import pytest
import polars as pl

import cryo


datatypes = [
    'blocks',
    'transactions',
    'txs',
    'logs',
    'contracts',
    'traces',
    'nonce_diffs',
    'balance_diffs',
    'storage_diffs',
    'code_diffs',
    'vm_traces',
]

block_ranges = [['17_000_000:17_000_010']]


@pytest.mark.parametrize('datatype', datatypes)
@pytest.mark.parametrize('blocks', block_ranges)
def test_datatype(datatype, blocks):
    output_dir = tempfile.mkdtemp()
    if datatype == 'vm_traces':
        blocks = ['17_000_000:17_000_001']
    cryo.collect(datatype, blocks=blocks)
    results = cryo.freeze(datatype, blocks=blocks, output_dir=output_dir)
    assert results['n_errored'] == 0


@pytest.mark.parametrize('datatype', datatypes)
@pytest.mark.parametrize('blocks', block_ranges)
def test_datatype_hex(datatype, blocks):
    if datatype == 'vm_traces':
        pytest.skip('')
        return
    df = cryo.collect(datatype, blocks=blocks, hex=False)
    assert len(df.select(pl.col(pl.Binary)).columns) > 0
    df = cryo.collect(datatype, blocks=blocks, hex=True)
    assert len(df.select(pl.col(pl.Binary)).columns) == 0


# def test_sort():
#     raise NotImplementedError()
