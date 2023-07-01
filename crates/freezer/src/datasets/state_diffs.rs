use std::collections::HashMap;

use ethers::prelude::*;
use polars::prelude::*;

use crate::chunks;
use crate::fetch;
use crate::types::BlockChunk;
use crate::types::CollectError;
use crate::types::FreezeOpts;
use crate::types::Schema;
use crate::types::Datatype;

pub async fn collect_single(
    datatype: &Datatype,
    block_chunk: &BlockChunk,
    opts: &FreezeOpts,
) -> Result<DataFrame, CollectError> {
    let block_numbers = chunks::get_chunk_block_numbers(block_chunk);
    let diffs = fetch::fetch_state_diffs(
        block_chunk,
        &opts.provider,
        &opts.max_concurrent_blocks,
    )
    .await?;
    match state_diffs_to_df(diffs, block_numbers, &opts.schemas) {
        Ok(mut dfs) => match dfs.remove(datatype) {
            Some(df) => Ok(df),
            None => Err(CollectError::BadSchemaError),
        },
        Err(e) => Err(CollectError::PolarsError(e)),
    }
}

pub fn state_diffs_to_df(
    blocks_traces: Vec<BlockTrace>,
    block_numbers: Vec<u64>,
    schemas: &HashMap<Datatype, Schema>,
) -> Result<HashMap<Datatype, DataFrame>, PolarsError> {

    let include_storage = schemas.contains_key(&Datatype::StorageDiffs);
    let include_balance = schemas.contains_key(&Datatype::BalanceDiffs);
    let include_nonce = schemas.contains_key(&Datatype::NonceDiffs);
    let include_code = schemas.contains_key(&Datatype::CodeDiffs);

    // storage
    let include_storage_block_number = included(schemas, Datatype::StorageDiffs, "block_number");
    let include_storage_transaction_hash = included(schemas, Datatype::StorageDiffs, "transaction_hash");
    let include_storage_address = included(schemas, Datatype::StorageDiffs, "address");
    let include_storage_slot = included(schemas, Datatype::StorageDiffs, "slot");
    let include_storage_from_value = included(schemas, Datatype::StorageDiffs, "from_value");
    let include_storage_to_value = included(schemas, Datatype::StorageDiffs, "to_value");
    let mut storage_block_number: Vec<u64> = Vec::with_capacity(blocks_traces.len());
    let mut storage_transaction_hash: Vec<Vec<u8>> = Vec::with_capacity(blocks_traces.len());
    let mut storage_address: Vec<Vec<u8>> = Vec::with_capacity(blocks_traces.len());
    let mut storage_slot: Vec<Vec<u8>> = Vec::with_capacity(blocks_traces.len());
    let mut storage_from_value: Vec<Vec<u8>> = Vec::with_capacity(blocks_traces.len());
    let mut storage_to_value: Vec<Vec<u8>> = Vec::with_capacity(blocks_traces.len());

    // balance
    let include_balance_block_number = included(schemas, Datatype::BalanceDiffs, "block_number");
    let include_balance_transaction_hash = included(schemas, Datatype::BalanceDiffs, "transaction_hash");
    let include_balance_address = included(schemas, Datatype::BalanceDiffs, "address");
    let include_balance_from_value = included(schemas, Datatype::BalanceDiffs, "from_value");
    let include_balance_to_value = included(schemas, Datatype::BalanceDiffs, "to_value");
    let mut balance_block_number: Vec<u64> = Vec::with_capacity(blocks_traces.len());
    let mut balance_transaction_hash: Vec<Vec<u8>> = Vec::with_capacity(blocks_traces.len());
    let mut balance_address: Vec<Vec<u8>> = Vec::with_capacity(blocks_traces.len());
    let mut balance_from_value: Vec<String> = Vec::with_capacity(blocks_traces.len());
    let mut balance_to_value: Vec<String> = Vec::with_capacity(blocks_traces.len());

    // nonce
    let include_nonce_block_number = included(schemas, Datatype::NonceDiffs, "block_number");
    let include_nonce_transaction_hash = included(schemas, Datatype::NonceDiffs, "transaction_hash");
    let include_nonce_address = included(schemas, Datatype::NonceDiffs, "address");
    let include_nonce_from_value = included(schemas, Datatype::NonceDiffs, "from_value");
    let include_nonce_to_value = included(schemas, Datatype::NonceDiffs, "to_value");
    let mut nonce_block_number: Vec<u64> = Vec::with_capacity(blocks_traces.len());
    let mut nonce_transaction_hash: Vec<Vec<u8>> = Vec::with_capacity(blocks_traces.len());
    let mut nonce_address: Vec<Vec<u8>> = Vec::with_capacity(blocks_traces.len());
    let mut nonce_from_value: Vec<u64> = Vec::with_capacity(blocks_traces.len());
    let mut nonce_to_value: Vec<u64> = Vec::with_capacity(blocks_traces.len());

    // code
    let include_code_block_number = included(schemas, Datatype::CodeDiffs, "block_number");
    let include_code_transaction_hash = included(schemas, Datatype::CodeDiffs, "transaction_hash");
    let include_code_address = included(schemas, Datatype::CodeDiffs, "address");
    let include_code_from_value = included(schemas, Datatype::CodeDiffs, "from_value");
    let include_code_to_value = included(schemas, Datatype::CodeDiffs, "to_value");
    let mut code_block_number: Vec<u64> = Vec::with_capacity(blocks_traces.len());
    let mut code_transaction_hash: Vec<Vec<u8>> = Vec::with_capacity(blocks_traces.len());
    let mut code_address: Vec<Vec<u8>> = Vec::with_capacity(blocks_traces.len());
    let mut code_from_value: Vec<Vec<u8>> = Vec::with_capacity(blocks_traces.len());
    let mut code_to_value: Vec<Vec<u8>> = Vec::with_capacity(blocks_traces.len());


    for (block_num, ts) in block_numbers.iter().zip(blocks_traces) {
        if let (Some(tx), Some(StateDiff(state_diff))) = (ts.transaction_hash, ts.state_diff) {
            for (addr, addr_diff) in state_diff.iter() {

                // storage
                if include_storage {
                    for (s, diff) in addr_diff.storage.iter() {
                        let (from, to) = match diff {
                            Diff::Same => (H256::zero(), H256::zero()),
                            Diff::Born(value) => (H256::zero(), *value),
                            Diff::Died(value) => (*value, H256::zero()),
                            Diff::Changed(ChangedType { from, to }) => (*from, *to),
                        };
                        if include_storage_block_number {
                            storage_block_number.push(*block_num);
                        };
                        if include_storage_transaction_hash {
                            storage_transaction_hash.push(tx.as_bytes().to_vec());
                        };
                        if include_storage_address {
                            storage_address.push(addr.as_bytes().to_vec());
                        };
                        if include_storage_slot {
                            storage_slot.push(s.as_bytes().to_vec());
                        };
                        if include_storage_from_value {
                            storage_from_value.push(from.as_bytes().to_vec());
                        };
                        if include_storage_to_value {
                            storage_to_value.push(to.as_bytes().to_vec());
                        };
                    }
                }

                // balance
                if include_balance {
                    let (from, to) = match addr_diff.balance {
                        Diff::Same => ("0".to_string(), "0".to_string()),
                        Diff::Born(value) => ("0".to_string(), value.to_string()),
                        Diff::Died(value) => (value.to_string(), "0".to_string()),
                        Diff::Changed(ChangedType { from, to }) => (from.to_string(), to.to_string()),
                    };
                    if include_balance_block_number {
                        balance_block_number.push(*block_num);
                    };
                    if include_balance_transaction_hash {
                        balance_transaction_hash.push(tx.as_bytes().to_vec());
                    };
                    if include_balance_address {
                        balance_address.push(addr.as_bytes().to_vec());
                    };
                    if include_balance_from_value {
                        balance_from_value.push(from);
                    };
                    if include_balance_to_value {
                        balance_to_value.push(to);
                    };
                }

                // nonce
                if include_nonce {
                    let (from, to) = match addr_diff.nonce {
                        Diff::Same => (0u64, 0u64),
                        Diff::Born(value) => (0u64, value.as_u64()),
                        Diff::Died(value) => (value.as_u64(), 0u64),
                        Diff::Changed(ChangedType { from, to }) => (from.as_u64(), to.as_u64()),
                    };
                    if include_nonce_block_number {
                        nonce_block_number.push(*block_num);
                    };
                    if include_nonce_transaction_hash {
                        nonce_transaction_hash.push(tx.as_bytes().to_vec());
                    };
                    if include_nonce_address {
                        nonce_address.push(addr.as_bytes().to_vec());
                    };
                    if include_nonce_from_value {
                        nonce_from_value.push(from);
                    };
                    if include_nonce_to_value {
                        nonce_to_value.push(to);
                    };
                }

                // code
                if include_code {
                    let (from, to) = match &addr_diff.code {
                        Diff::Same => (H256::zero().as_bytes().to_vec(), H256::zero().as_bytes().to_vec()),
                        Diff::Born(value) => (H256::zero().as_bytes().to_vec(), value.to_vec()),
                        Diff::Died(value) => (value.to_vec(), H256::zero().as_bytes().to_vec()),
                        Diff::Changed(ChangedType { from, to }) => (from.to_vec(), to.to_vec()),
                    };
                    if include_code_block_number {
                        code_block_number.push(*block_num);
                    };
                    if include_code_transaction_hash {
                        code_transaction_hash.push(tx.as_bytes().to_vec());
                    };
                    if include_code_address {
                        code_address.push(addr.as_bytes().to_vec());
                    };
                    if include_code_from_value {
                        code_from_value.push(from);
                    };
                    if include_code_to_value {
                        code_to_value.push(to);
                    };
                }

            }
        }
    }

    let mut dfs: HashMap<Datatype, DataFrame> = HashMap::new();

    // storage
    if include_storage {
        let mut cols = Vec::new();
        if include_storage_block_number {
            cols.push(Series::new("block_number", storage_block_number));
        }
        if include_storage_transaction_hash {
            cols.push(Series::new("transaction_hash", storage_transaction_hash));
        }
        if include_storage_address {
            cols.push(Series::new("address", storage_address));
        }
        if include_storage_slot {
            cols.push(Series::new("slot", storage_slot));
        }
        if include_storage_from_value {
            cols.push(Series::new("from_value", storage_from_value));
        }
        if include_storage_to_value {
            cols.push(Series::new("to_value", storage_to_value));
        }
        let df = DataFrame::new(cols)?;
        dfs.insert(Datatype::StorageDiffs, df);
    };

    // balance
    if include_balance {
        let mut cols = Vec::new();
        if include_balance_block_number {
            cols.push(Series::new("block_number", balance_block_number));
        }
        if include_balance_transaction_hash {
            cols.push(Series::new("transaction_hash", balance_transaction_hash));
        }
        if include_balance_address {
            cols.push(Series::new("address", balance_address));
        }
        if include_balance_from_value {
            cols.push(Series::new("from_value", balance_from_value));
        }
        if include_balance_to_value {
            cols.push(Series::new("to_value", balance_to_value));
        }
        let df = DataFrame::new(cols)?;
        dfs.insert(Datatype::BalanceDiffs, df);
    };

    // nonce
    if include_nonce {
        let mut cols = Vec::new();
        if include_nonce_block_number {
            cols.push(Series::new("block_number", nonce_block_number));
        }
        if include_nonce_transaction_hash {
            cols.push(Series::new("transaction_hash", nonce_transaction_hash));
        }
        if include_nonce_address {
            cols.push(Series::new("address", nonce_address));
        }
        if include_nonce_from_value {
            cols.push(Series::new("from_value", nonce_from_value));
        }
        if include_nonce_to_value {
            cols.push(Series::new("to_value", nonce_to_value));
        }
        let df = DataFrame::new(cols)?;
        dfs.insert(Datatype::NonceDiffs, df);
    };

    // code
    if include_code {
        let mut cols = Vec::new();
        if include_code_block_number {
            cols.push(Series::new("block_number", code_block_number));
        }
        if include_code_transaction_hash {
            cols.push(Series::new("transaction_hash", code_transaction_hash));
        }
        if include_code_address {
            cols.push(Series::new("address", code_address));
        }
        if include_code_from_value {
            cols.push(Series::new("from_value", code_from_value));
        }
        if include_code_to_value {
            cols.push(Series::new("to_value", code_to_value));
        }
        let df = DataFrame::new(cols)?;
        dfs.insert(Datatype::CodeDiffs, df);
    };

    Ok(dfs)
}

fn included(
    schemas: &HashMap<Datatype, Schema>,
    datatype: Datatype,
    column_name: &'static str,
) -> bool {
    if let Some(schema) = schemas.get(&datatype) {
        schema.contains_key(column_name)
    } else {
        false
    }
}

