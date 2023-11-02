# ❄️🧊 cryo 🧊❄️

[![Rust](https://github.com/paradigmxyz/cryo/actions/workflows/build_and_test.yml/badge.svg)](https://github.com/paradigmxyz/cryo/actions/workflows/build_and_test.yml)

`cryo` is the easiest way to extract blockchain data to parquet, csv, json, or a python dataframe.

`cryo` is also extremely flexible, with [many different options](#cli-options) to control how data is extracted + filtered + formatted

*`cryo` is an early WIP, please report bugs + feedback to the issue tracker*

*note that `cryo`'s default settings will slam a node too hard for use with 3rd party RPC providers. Instead, `--requests-per-second` and `--max-concurrent-requests` should be used to impose ratelimits. Such settings will be handled automatically in a future release*.

## Contents

1. [Example Usage](#example-usage)
2. [Installation](#installation)
3. [Data Schema](#data-schemas)
4. [Code Guide](#code-guide)
5. [Documenation](#documentation)
    1. [Basics](#cryo-help)
    2. [Syntax](#cryo-syntax)
    3. [Datasets](#cryo-datasets)

## Example Usage

use as `cryo <dataset> [OPTIONS]`

| Example | Command |
| :- | :- |
| Extract all logs from block 16,000,000 to block 17,000,000 | `cryo logs -b 16M:17M` |
| Extract blocks, logs, or traces missing from current directory | `cryo blocks txs traces` |
| Extract to csv instead of parquet | `cryo blocks txs traces --csv` |
| Extract only certain columns | `cryo blocks --include number timestamp` |
| Dry run to view output schemas or expected work | `cryo storage_diffs --dry` |
| Extract all USDC events | `cryo logs --contract 0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48` |

`cryo` uses `ETH_RPC_URL` env var as the data source unless `--rpc <url>` is given

## Installation

#### Method 1: install from source

```bash
git clone https://github.com/paradigmxyz/cryo
cd cryo
cargo install --path ./crates/cli
```

This method requires having rust installed. See [rustup](https://rustup.rs/) for instructions.

#### Method 2: install from crates.io

```bash
cargo install cryo_cli
```

This method requires having rust installed. See [rustup](https://rustup.rs/) for instructions.

Make sure that `~/.cargo/bin` is on your `PATH`. One way to do this is by adding the line `export PATH="$HOME/.cargo/bin:$PATH"` to your `~/.bashrc` or `~/.profile`.

#### Installing `cryo_python` from pypi

(make sure rust is installed first, see [rustup](https://www.rust-lang.org/tools/install))

```bash
pip install maturin
pip install cryo_python
```

#### Installing `cryo_python` from source

```bash
pip install maturin
git clone https://github.com/paradigmxyz/cryo
cd cryo/crates/python
maturin build --release
pip install --force-reinstall <OUTPUT_OF_MATURIN_BUILD>.whl
```

## Data Schemas

Many `cryo` cli options will affect output schemas by adding/removing columns or changing column datatypes.

`cryo` will always print out data schemas before collecting any data. To view these schemas without collecting data, use `--dry` to perform a dry run.

#### Schema Design Guide

An attempt is made to ensure that the dataset schemas conform to a common set of design guidelines:
- By default, rows should contain enough information in their columns to be order-able (unless the rows do not have an intrinsic order).
- Columns should usually be named by their JSON-RPC or ethers.rs defaults, except in cases where a much more explicit name is available.
- To make joins across tables easier, a given piece of information should use the same datatype and column name across tables when possible.
- Large ints such as `u256` should allow multiple conversions. A `value` column of type `u256` should allow: `value_binary`, `value_string`, `value_f32`, `value_f64`, `value_u32`, `value_u64`, and `value_d128`. These types can be specified at runtime using the `--u256-types` argument.
- By default, columns related to non-identifying cryptographic signatures are omitted by default. For example, `state_root` of a block or `v`/`r`/`s` of a transaction.
- Integer values that can never be negative should be stored as unsigned integers.
- Every table should allow a `chain_id` column so that data from multiple chains can be easily stored in the same table.

Standard types across tables:
- `block_number`: `u32`
- `transaction_index`: `u32`
- `nonce`: `u32`
- `gas_used`: `u64`
- `gas_limit`: `u64`
- `chain_id`: `u64`
- `timestamp`: `u32`

#### JSON-RPC

`cryo` currently obtains all of its data using the [JSON-RPC](https://ethereum.org/en/developers/docs/apis/json-rpc/) protocol standard.

|dataset|blocks per request|results per block|method|
|-|-|-|-|
|Blocks|1|1|`eth_getBlockByNumber`|
|Transactions|1|multiple|`eth_getBlockByNumber`|
|Logs|multiple|multiple|`eth_getLogs`|
|Contracts|1|multiple|`trace_block`|
|Traces|1|multiple|`trace_block`|
|State Diffs|1|multiple|`trace_replayBlockTransactions`|
|Vm Traces|1|multiple|`trace_replayBlockTransactions`|

`cryo` use [ethers.rs](https://github.com/gakonst/ethers-rs) to perform JSON-RPC requests, so it can be used any chain that ethers-rs is compatible with. This includes Ethereum, Optimism, Arbitrum, Polygon, BNB, and Avalanche.

A future version of `cryo` will be able to bypass JSON-RPC and query node data directly.

## Code Guide
- Code is arranged into the following crates:
    - `cryo_cli`: convert textual data into cryo function calls
    - `cryo_freeze`: core cryo code
    - `cryo_python`: cryo python adapter
    - `cryo_to_df`: procedural macro for generating dataset definitions
- Do not use panics (including `panic!`, `todo!`, `unwrap()`, and `expect()`) except in the following circumstances: tests, build scripts, lazy static blocks, and procedural macros

## Documentation

1. [cryo help](#cryo-help)
2. [cryo syntax](#cryo-syntax)
3. [cryo datasets](#cryo-datasets)

#### cryo help

(output of `cryo help`)

```
cryo extracts blockchain data to parquet, csv, or json

Usage: cryo [OPTIONS] <DATATYPE>...

Arguments:
  <DATATYPE>...  datatype(s) to collect, use cryo datasets to see all available

Options:
      --no-verbose  Run quietly without printing information to stdout
  -h, --help        Print help
  -V, --version     Print version

Content Options:
  -b, --blocks <BLOCKS>...           Block numbers, see syntax below
  -t, --txs <TXS>...                 Transaction hashes, see syntax below
  -a, --align                        Align chunk boundaries to regular intervals,
                                     e.g. (1000 2000 3000), not (1106 2106 3106)
      --reorg-buffer <N_BLOCKS>      Reorg buffer, save blocks only when this old,
                                     can be a number of blocks [default: 0]
  -i, --include-columns [<COLS>...]  Columns to include alongside the defaults,
                                     use `all` to include all available columns
  -e, --exclude-columns [<COLS>...]  Columns to exclude from the defaults
      --columns [<COLS>...]          Columns to use instead of the defaults,
                                     use `all` to use all available columns
      --u256-types <U256_TYPES>...   Set output datatype(s) of U256 integers
                                     [default: binary, string, f64]
      --hex                          Use hex string encoding for binary columns
  -s, --sort [<SORT>...]             Columns(s) to sort by, `none` for unordered

Source Options:
  -r, --rpc <RPC>                    RPC url [default: ETH_RPC_URL env var]
      --network-name <NETWORK_NAME>  Network name [default: name of eth_getChainId]

Acquisition Options:
  -l, --requests-per-second <limit>  Ratelimit on requests per second
      --max-retries <R>              Max retries for provider errors [default: 5]
      --initial-backoff <B>          Initial retry backoff time (ms) [default: 500]
      --max-concurrent-requests <M>  Global number of concurrent requests
      --max-concurrent-chunks <M>    Number of chunks processed concurrently
  -d, --dry                          Dry run, collect no data

Output Options:
  -c, --chunk-size <CHUNK_SIZE>      Number of blocks per file [default: 1000]
      --n-chunks <N_CHUNKS>          Number of files (alternative to --chunk-size)
      --partition-by <PARTITION_BY>  Dimensions to partition by
  -o, --output-dir <OUTPUT_DIR>      Directory for output files [default: .]
      --subdirs <SUBDIRS>...         Subdirectories for output files
                                     can be `datatype`, `network`, or custom string
      --file-suffix <FILE_SUFFIX>    Suffix to attach to end of each filename
      --overwrite                    Overwrite existing files instead of skipping
      --csv                          Save as csv instead of parquet
      --json                         Save as json instead of parquet
      --row-group-size <GROUP_SIZE>  Number of rows per row group in parquet file
      --n-row-groups <N_ROW_GROUPS>  Number of rows groups in parquet file
      --no-stats                     Do not write statistics to parquet files
      --compression <NAME [#]>...    Compression algorithm and level [default: lz4]
      --report-dir <REPORT_DIR>      Directory to save summary report
                                     [default: {output_dir}/.cryo/reports]
      --no-report                    Avoid saving a summary report

Dataset-specific Options:
      --address <ADDRESS>...         Address(es)
      --to-address <address>...      To Address(es)
      --from-address <address>...    From Address(es)
      --call-data <CALL_DATA>...     Call data(s) to use for eth_calls
      --function <FUNCTION>...       Function(s) to use for eth_calls
      --inputs <INPUTS>...           Input(s) to use for eth_calls
      --slot <SLOT>...               Slot(s)
      --contract <CONTRACT>...       Contract address(es)
      --topic0 <TOPIC0>...           Topic0(s) [aliases: event]
      --topic1 <TOPIC1>...           Topic1(s)
      --topic2 <TOPIC2>...           Topic2(s)
      --topic3 <TOPIC3>...           Topic3(s)
      --event-signature <SIG>...     Event signature for log decoding
      --inner-request-size <BLOCKS>  Blocks per request (eth_getLogs) [default: 1]

Optional Subcommands:
      cryo help                      display help message
      cryo help syntax               display block + tx specification syntax
      cryo help datasets             display list of all datasets
      cryo help <DATASET(S)>         display info about a dataset
```

#### cryo syntax

(output of `cryo help syntax`)

```
Block specification syntax
- can use numbers                    --blocks 5000 6000 7000
- can use ranges                     --blocks 12M:13M 15M:16M
- can use a parquet file             --blocks ./path/to/file.parquet[:COLUMN_NAME]
- can use multiple parquet files     --blocks ./path/to/files/*.parquet[:COLUMN_NAME]
- numbers can contain { _ . K M B }  5_000 5K 15M 15.5M
- omitting range end means latest    15.5M: == 15.5M:latest
- omitting range start means 0       :700 == 0:700
- minus on start means minus end     -1000:7000 == 6000:7000
- plus sign on end means plus start  15M:+1000 == 15M:15.001K
- can use every nth value            2000:5000:1000 == 2000 3000 4000
- can use n values total             100:200/5 == 100 124 149 174 199

Transaction specification syntax
- can use transaction hashes         --txs TX_HASH1 TX_HASH2 TX_HASH3
- can use a parquet file             --txs ./path/to/file.parquet[:COLUMN_NAME]
                                     (default column name is transaction_hash)
- can use multiple parquet files     --txs ./path/to/ethereum__logs*.parquet
```

#### cryo datasets

(output of `cryo help datasets`)

```
cryo datasets
─────────────
- address_appearances
- balance_diffs
- balances
- blocks
- code_diffs
- codes
- contracts
- erc20_balances
- erc20_metadata
- erc20_supplies
- erc20_transfers
- erc721_metadata
- erc721_transfers
- eth_calls
- geth_code_diffs
- geth_balance_diffs
- geth_storage_diffs
- geth_nonce_diffs
- geth_traces
- logs
- native_transfers
- nonce_diffs
- nonces
- slots
- storage_diffs
- traces
- trace_calls
- transactions
- vm_traces

dataset group names
───────────────────
- blocks_and_transactions: blocks, transactions
- call_trace_derivatives: contracts, native_transfers, traces
- geth_state_diffs: geth_balance_diffs, geth_code_diffs, geth_nonce_diffs, geth_storage_diffs
- state_diffs: balance_diffs, code_diffs, nonce_diffs, storage_diffs

use cryo help <DATASET> to print info about a specific dataset
```
