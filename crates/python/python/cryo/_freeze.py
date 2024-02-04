from __future__ import annotations

import typing

if typing.TYPE_CHECKING:
    from typing_extensions import Unpack

    from . import _spec


async def async_freeze(
    datatype: str | typing.Sequence[str],
    **kwargs: Unpack[_spec.CryoCliArgs],
) -> None:
    """
    Asynchronously collect data and save to disk

    see cryo.parse_kwargs() for descriptions of arguments
    """

    from . import _cryo_rust  # type: ignore
    from . import _args

    if isinstance(datatype, str):
        datatypes = [datatype]
    elif isinstance(datatype, list):
        datatypes = datatype
    else:
        raise Exception("Invalid format for datatype(s)")

    cli_args = _args.parse_cli_args(**kwargs)
    return await _cryo_rust._freeze(datatypes, **cli_args)  # type: ignore


def freeze(
    datatype: str | typing.Sequence[str],
    **kwargs: Unpack[_spec.CryoCliArgs],
) -> None:
    """Freeze collects data and saves it to a specified disk.

    Parameters
    ----------
    datatype: str | [str]
       (Required) Specifies the dataset(s) to be collected. Possible values include:

       cryo datasets
       -------------
            - address_appearances
            - balance_diffs
            - balance_reads
            - balances
            - blocks
            - code_diffs
            - code_reads
            - codes
            - contracts
            - erc20_balances
            - erc20_metadata
            - erc20_supplies
            - erc20_transfers
            - erc721_metadata
            - erc721_transfers
            - eth_calls,
            - four_byte_counts (alias=4byte_counts)
            - geth_calls
            - geth_code_diffs
            - geth_balance_diffs
            - geth_storage_diffs
            - geth_nonce_diffs
            - geth_opcodes,
            - javascript_traces (alias=js_traces)
            - logs (alias=events)
            - native_transfers
            - nonce_diffs
            - nonce_reads
            - nonces
            - slots (alias=storages)
            - storage_diffs (alias=slot_diffs)
            - storage_reads (alias=slot_reads)
            - traces    print(f"Arguments passed to freeze: {kwargs}")
            - trace_calls
            - transactions (alias=txs)
            - vm_traces (alias=opcode_traces)

        dataset group names
        -------------------
            - blocks_and_transactions: blocks, transactions
            - call_trace_derivatives: contracts, native_transfers, traces
            - geth_state_diffs: geth_balance_diffs, geth_code_diffs, geth_nonce_diffs, geth_storage_diffs
            - state_diffs: balance_diffs, code_diffs, nonce_diffs, storage_diffs
            - state_reads: balance_reads, code_reads, nonce_reads, storage_reads

    blocks: [str] | None
        Specifies the blocks to be collected during data collection.
        Default Value: ["0:latest"]
          - list of numbers                         ["5000" "6000" "7000"]
          - ranges of numbers                       ["12M:13M" "15M:16M"]
          - omitting range end means latest         ["15.5M:"] == ["15.5M:latest"]
          - omitting range start means 0            [":700"] == ["0:700"]
          - minus sign on start means minus end     ["-1000:7000"] == ["6001:7001"]
          - plus sign on end means plus start       ["15M:+1000"] == ["15M:15.001"]
          - can use every nth value                 ["2000:5000:1000"] == ["2000", "3000", "4000"]
          - can use n values total                  ["100:200/5"]
          - number can contrain { _ . K M B }       ["5_000", "6K", "7M", "15_500K", "0.017525B"]

    align: bool
        Align chunk boundaries to regular intervals, e.g. (1000 2000 3000), not (1106 2106 3106)
        Default: False

    reorg_buffer: int
        Only save blocks that are <N_BLOCKS> old, can be any number of blocks
        Default: 0

    include_columns: [str]
        Columns to include alongside the defaults.
        Use "all" to include all available columns.
        Default: Varies with datatype

    exclude_columns: [str]
        Columns to exclude from result.
        Note: If "all" is selected from `include_columns` then `exclude_columns` is ignored.

    columns: [str]
        Columns to return instead of returning the default columns for the datatype.
        Use "all" to get all available columns.

    hex: bool
        Use hex string encoding for binary columns.
        Default: False

    u256_types:

    sort:

    exclude_failed:

    rpc:

    network_name:

    requests_per_second:

    max_concurrent_requests:

    max_concurrent_chunks:

    dry:

    chunk_size:

    n_chunks:

    output_dir:

    file_suffix:

    overwrite:

    csv:

    json:

    row_group_size:

    n_row_groups:

    no_stats:

    compression:

    contract:

    topic0: [str]

    topic1: [str]

    topic2: [str]

    topic3: [str]

    inner_request_size: [int]

    js_tracer:

    verbose: bool

    no_verbose: bool

    event_signature:

    remember: bool

    """

    import asyncio

    coroutine = async_freeze(datatype, **kwargs)

    try:
        import concurrent.futures

        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        with concurrent.futures.ThreadPoolExecutor() as executor:
            future = executor.submit(loop.run_until_complete, coroutine)  # type: ignore
            return future.result()  # type: ignore
    except RuntimeError:
        return asyncio.run(coroutine)
