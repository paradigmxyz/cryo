# ❄️🧊 cryo 🧊❄️

[![Rust](https://github.com/paradigmxyz/cryo/actions/workflows/build_and_test.yml/badge.svg)](https://github.com/paradigmxyz/cryo/actions/workflows/build_and_test.yml)

`cryo` is the easiest way to extract blockchain data to parquet, csv, json, or a python dataframe.

`cryo` is also extremely flexible, with [many different options](#cli-options) to control how data is extracted + filtered + formatted

*`cryo` is an early WIP, please report bugs + feedback to the issue tracker*

*note that `cryo`'s default settings will slam a node too hard for use with 3rd party RPC providers. Instead, `--requests-per-second` and `--max-concurrent-requests` should be used to impose ratelimits. Such settings will be handled automatically in a future release*.

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

## Datasets

cryo can extract the following datasets from EVM nodes:
- `blocks`
- `transactions` (alias = `txs`)
- `logs` (alias = `events`)
- `contracts`
- `traces` (alias = `call_traces`)
- `state_diffs` (alias for `storage_diffs` + `balance_diff` + `nonce_diffs` + `code_diffs`)
- `balance_diffs`
- `code_diffs`
- `storage_diffs`
- `nonce_diffs`
- `vm_traces` (alias = `opcode_traces`)

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
pip install <OUTPUT_OF_MATURIN_BUILD>.whl
```

## Data Schema

Many `cryo` cli options will affect output schemas by adding/removing columns or changing column datatypes.

`cryo` will always print out data schemas before collecting any data. To view these schemas without collecting data, use `--dry` to perform a dry run.

#### Schema Design Guide

An attempt is made to ensure that the dataset schemas conform to a common set of design guidelines:
- By default, rows should contain enough information in their columns to be order-able (unless the rows do not have an intrinsic order)
- Columns should be named by their JSON-RPC or ethers.rs defaults, except in cases where a much more explicit name is available
- To make joins across tables easier, a given piece of information should use the same datatype and column name across tables when possible
- Large ints such as `u256` should allow multiple conversions. A `value` column of type `u256` should allow: `value_binary`, `value_string`, `value_f32`, `value_f64`, `value_u32`, `value_u64`, and `value_d128`
- By default, columns related to non-identifying cryptographic signatures are omitted by default. For example, `state_root` of a block or `v`/`r`/`s` of a transaction
- Integer values that can never be negative should be stored as unsigned integers
- Every table should allow an optional `chain_id` column so that data from multiple chains can be easily stored in the same table.

Standard types across tables:
- `block_number`: `u32`
- `transaction_index`: `u32`
- `nonce`: `u32`
- `gas_used`: `u32`
- `gas_limit`: `u32`
- `chain_id`: `u64`

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

## CLI Options

output of `cryo --help`:

```
cryo extracts blockchain data to parquet, csv, or json

Usage: cryo [OPTIONS] <DATATYPE>...

Arguments:
  <DATATYPE>...  datatype(s) to collect, one or more of:
                 - blocks
                 - transactions  (alias = txs)
                 - logs          (alias = events)
                 - contracts
                 - traces        (alias = call_traces)
                 - state_diffs   (= balance + code + nonce + storage diffs)
                 - balance_diffs
                 - code_diffs
                 - nonce_diffs
                 - storage_diffs
                 - vm_traces     (alias = opcode_traces)

Options:
  -h, --help     Print help
  -V, --version  Print version

Content Options:
  -b, --blocks <BLOCKS>              Block numbers, see syntax below [default: 0:latest]
  -a, --align                        Align block chunk boundaries to regular intervals
                                     e.g. (1000, 2000, 3000) instead of (1106, 2106, 3106)
      --reorg-buffer <N_BLOCKS>      Reorg buffer, save blocks only when they are this old,
                                     can be a number of blocks [default: 0]
  -i, --include-columns [<COLS>...]  Columns to include alongside the default output
  -e, --exclude-columns [<COLS>...]  Columns to exclude from the default output
      --columns [<COLS>...]          Use these columns instead of the default
      --hex                          Use hex string encoding for binary columns
  -s, --sort [<SORT>...]             Columns(s) to sort by

Source Options:
  -r, --rpc <RPC>                    RPC url [default: ETH_RPC_URL env var]
      --network-name <NETWORK_NAME>  Network name [default: use name of eth_getChainId]

Acquisition Options:
  -l, --requests-per-second <limit>  Ratelimit on requests per second
      --max-concurrent-requests <M>  Global number of concurrent requests
      --max-concurrent-chunks <M>    Number of chunks processed concurrently
      --max-concurrent-blocks <M>    Number blocks within a chunk processed concurrently
  -d, --dry                          Dry run, collect no data

Output Options:
  -c, --chunk-size <CHUNK_SIZE>      Number of blocks per file [default: 1000]
      --n-chunks <N_CHUNKS>          Number of files (alternative to --chunk-size)
  -o, --output-dir <OUTPUT_DIR>      Directory for output files [default: .]
      --file-suffix <FILE_SUFFIX>    Suffix to attach to end of each filename
      --overwrite                    Overwrite existing files instead of skipping them
      --csv                          Save as csv instead of parquet
      --json                         Save as json instead of parquet
      --row-group-size <GROUP_SIZE>  Number of rows per row group in parquet file
      --n-row-groups <N_ROW_GROUPS>  Number of rows groups in parquet file
      --no-stats                     Do not write statistics to parquet files
      --compression <NAME [#]>...    Set compression algorithm and level [default: lz4]

Dataset-specific Options:
      --contract <CONTRACT>          [logs] filter logs by contract address
      --topic0 <TOPIC0>              [logs] filter logs by topic0 [aliases: event]
      --topic1 <TOPIC1>              [logs] filter logs by topic1
      --topic2 <TOPIC2>              [logs] filter logs by topic2
      --topic3 <TOPIC3>              [logs] filter logs by topic3
      --log-request-size <N_BLOCKS>  [logs] Number of blocks per log request [default: 1]


Block specification syntax
- can use numbers                    --blocks 5000
- can use numbers list (use "")      --blocks "5000 6000 7000"
- can use ranges                     --blocks 12M:13M 15M:16M
- numbers can contain { _ . K M B }  5_000 5K 15M 15.5M
- omiting range end means latest     15.5M: == 15.5M:latest
- omitting range start means 0       :700 == 0:700
- minus on start means minus end     -1000:7000 == 6000:7000
- plus sign on end means plus start  15M:+1000 == 15M:15.001K
- mix formats                        "15M:+1 1000:1002 -3:1b 2000"
```

