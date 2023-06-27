# ❄️🧊 cryo 🧊❄️

cryo aims to be the quickest + easiest way to extract blockchain data to parquet, csv, or json

## Example Usage

use as `cryo <dataset> [OPTIONS]`

| Example | Command |
| :- | :- |
| Extract any blocks and transactions missing from current directory | `cryo blocks txs` |
| Extract all logs from block 16,000,000 to block 17,000,000 | `cryo logs -b 16M:17M` |
| Extract to csv | `cryo traces --csv` |
| Extract only certain columns | `cryo blocks --include number timestamp` |
| Dry run to view output schemas | `cryo storage_diffs --dry` |

## Datasets

cryo can extract the following datasets from EVM nodes:
- `blocks`
- `transactions` (alias = `txs`)
- `logs`
- `call_traces` (alias = `traces`)
- `storage_diffs` (alias for `slot_diffs` + `balance_diff` + `nonce_diffs` + `code_diffs`)
- `slot_diffs`
- `balance_diffs`
- `nonce_diffs`
- `code_diffs`
- `opcode_traces`

## Installation

[wip]

## CLI Options

```
cryo extracts blockchain data to parquet, csv, or json

Usage: cryo [OPTIONS] <DATATYPE>...

Arguments:
  <DATATYPE>...  datatype(s) to collect, one or more of:
                 - blocks
                 - logs
                 - transactions
                 - call_traces
                 - state_diffs
                 - balance_diffs
                 - code_diffs
                 - slot_diffs
                 - nonce_diffs
                 - opcode_traces

Options:
  -h, --help     Print help
  -V, --version  Print version

Content Options:
  -b, --blocks [<BLOCKS>...]         Block numbers, either numbers or start:end ranges [default: 17000000:17000100]
  -i, --include-columns [<COLS>...]  Columns to include in output
  -e, --exclude-columns [<COLS>...]  Columns to exclude from output

Source Options:
  -r, --rpc <RPC>                    RPC URL
      --network-name <NETWORK_NAME>  Network name, by default will derive from eth_getChainId

Acquisition Options:
      --max-concurrent-requests <M>  Global number of concurrent requests
      --max-concurrent-chunks <M>    Number of chunks processed concurrently
      --max-concurrent-blocks <M>    Number blocks within a chunk processed concurrently
      --log-request-size <SIZE>      Number of blocks per log request [default: 1]
  -d, --dry                          Dry run, collect no data

Output Options:
  -c, --chunk-size <CHUNK_SIZE>      Chunk size (blocks per chunk) [default: 1000]
      --n-chunks <N_CHUNKS>          Number of chunks (alternative to --chunk-size)
  -o, --output-dir <OUTPUT_DIR>      Directory for output files [default: .]
      --csv                          Save as csv instead of parquet
      --json                         Save as json instead of parquet
      --hex                          Use hex string encoding for binary columns
  -s, --sort [<SORT>...]             Columns(s) to sort by
      --row-groups <ROW_GROUPS>      Number of rows groups in parquet file
      --row-group-size <GROUP_SIZE>  Number of rows per row group in parquet file
      --no-stats                     Do not write statistics to parquet files
```
