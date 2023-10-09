use crate::*;
use ethers::prelude::*;
use polars::prelude::*;
use std::collections::HashMap;

/// columns for transactions
#[cryo_to_df::to_df(Datatype::StorageDiffs)]
#[derive(Default)]
pub struct StorageDiffs {
    n_rows: u64,
    block_number: Vec<Option<u32>>,
    transaction_index: Vec<Option<u64>>,
    transaction_hash: Vec<Option<Vec<u8>>>,
    address: Vec<Vec<u8>>,
    slot: Vec<Vec<u8>>,
    from_value: Vec<Vec<u8>>,
    to_value: Vec<Vec<u8>>,
    chain_id: Vec<u64>,
}

#[async_trait::async_trait]
impl Dataset for StorageDiffs {
    fn name() -> &'static str {
        "storage_diffs"
    }

    fn default_sort() -> Vec<String> {
        vec!["block_number".to_string(), "transaction_index".to_string()]
    }
}

type BlockTxsTraces = (Option<u32>, Vec<Option<Vec<u8>>>, Vec<ethers::types::BlockTrace>);
type Result<T> = ::core::result::Result<T, CollectError>;

#[async_trait::async_trait]
impl CollectByBlock for StorageDiffs {
    type Response = BlockTxsTraces;

    async fn extract(request: Params, source: Source, schemas: Schemas) -> Result<Self::Response> {
        let schema = schemas.get(&Datatype::StorageDiffs).ok_or(err("schema not provided"))?;
        let include_txs = schema.has_column("transaction_hash");
        source.fetcher.trace_block_state_diffs(request.block_number()? as u32, include_txs).await
    }

    fn transform(response: Self::Response, columns: &mut Self, schemas: &Schemas) -> Result<()> {
        process_storage_diffs(&response, columns, schemas)
    }
}

#[async_trait::async_trait]
impl CollectByTransaction for StorageDiffs {
    type Response = BlockTxsTraces;

    async fn extract(request: Params, source: Source, _schemas: Schemas) -> Result<Self::Response> {
        source.fetcher.trace_transaction_state_diffs(request.transaction_hash()?).await
    }

    fn transform(response: Self::Response, columns: &mut Self, schemas: &Schemas) -> Result<()> {
        process_storage_diffs(&response, columns, schemas)
    }
}

pub(crate) fn process_storage_diffs(
    response: &BlockTxsTraces,
    columns: &mut StorageDiffs,
    schemas: &Schemas,
) -> Result<()> {
    let schema = schemas.get(&Datatype::StorageDiffs).ok_or(err("schema not provided"))?;
    let (block_number, txs, traces) = response;
    for (index, (trace, tx)) in traces.iter().zip(txs).enumerate() {
        if let Some(ethers::types::StateDiff(state_diffs)) = &trace.state_diff {
            for (addr, diff) in state_diffs.iter() {
                process_storage_diff(addr, &diff.storage, block_number, tx, index, columns, schema);
            }
        }
    }
    Ok(())
}

pub(crate) fn process_storage_diff(
    addr: &H160,
    diff: &std::collections::BTreeMap<H256, Diff<H256>>,
    block_number: &Option<u32>,
    transaction_hash: &Option<Vec<u8>>,
    transaction_index: usize,
    columns: &mut StorageDiffs,
    schema: &Table,
) {
    for (s, sub_diff) in diff.iter() {
        columns.n_rows += 1;
        let (from, to) = match sub_diff {
            Diff::Same => (H256::zero(), H256::zero()),
            Diff::Born(value) => (H256::zero(), *value),
            Diff::Died(value) => (*value, H256::zero()),
            Diff::Changed(ChangedType { from, to }) => (*from, *to),
        };
        store!(schema, columns, block_number, *block_number);
        store!(schema, columns, transaction_index, Some(transaction_index as u64));
        store!(schema, columns, transaction_hash, transaction_hash.clone());
        store!(schema, columns, slot, s.as_bytes().to_vec());
        store!(schema, columns, address, addr.as_bytes().to_vec());
        store!(schema, columns, from_value, from.as_bytes().to_vec());
        store!(schema, columns, to_value, to.as_bytes().to_vec());
    }
}
