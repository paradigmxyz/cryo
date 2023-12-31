use crate::*;
use ethers::prelude::*;
use polars::prelude::*;
use std::collections::BTreeMap;

/// columns for transactions
#[cryo_to_df::to_df(Datatype::StorageReads)]
#[derive(Default)]
pub struct StorageReads {
    pub(crate) n_rows: u64,
    pub(crate) block_number: Vec<Option<u32>>,
    pub(crate) transaction_index: Vec<Option<u32>>,
    pub(crate) transaction_hash: Vec<Option<Vec<u8>>>,
    pub(crate) contract_address: Vec<Vec<u8>>,
    pub(crate) slot: Vec<Vec<u8>>,
    pub(crate) value: Vec<Vec<u8>>,
    pub(crate) chain_id: Vec<u64>,
}

#[async_trait::async_trait]
impl Dataset for StorageReads {
    fn aliases() -> Vec<&'static str> {
        vec!["slot_reads"]
    }
}

type BlockTxsTraces = (Option<u32>, Vec<Option<Vec<u8>>>, Vec<BTreeMap<H160, AccountState>>);

#[async_trait::async_trait]
impl CollectByBlock for StorageReads {
    type Response = BlockTxsTraces;

    async fn extract(request: Params, source: Arc<Source>, query: Arc<Query>) -> R<Self::Response> {
        let schema =
            query.schemas.get(&Datatype::StorageReads).ok_or(err("schema not provided"))?;
        let include_txs = schema.has_column("transaction_hash");
        source.geth_debug_trace_block_prestate(request.block_number()? as u32, include_txs).await
    }

    fn transform(response: Self::Response, columns: &mut Self, query: &Arc<Query>) -> R<()> {
        process_storage_reads(&response, columns, &query.schemas)
    }
}

#[async_trait::async_trait]
impl CollectByTransaction for StorageReads {
    type Response = BlockTxsTraces;

    async fn extract(request: Params, source: Arc<Source>, query: Arc<Query>) -> R<Self::Response> {
        let schema =
            query.schemas.get(&Datatype::StorageReads).ok_or(err("schema not provided"))?;
        let include_block_number = schema.has_column("block_number");
        let tx = request.transaction_hash()?;
        source.geth_debug_trace_transaction_prestate(tx, include_block_number).await
    }

    fn transform(response: Self::Response, columns: &mut Self, query: &Arc<Query>) -> R<()> {
        process_storage_reads(&response, columns, &query.schemas)
    }
}

pub(crate) fn process_storage_reads(
    response: &BlockTxsTraces,
    columns: &mut StorageReads,
    schemas: &Schemas,
) -> R<()> {
    let schema = schemas.get(&Datatype::StorageReads).ok_or(err("schema not provided"))?;
    let (block_number, txs, traces) = response;
    for (index, (trace, tx)) in traces.iter().zip(txs).enumerate() {
        for (addr, account_state) in trace.iter() {
            process_storage_read(addr, account_state, block_number, tx, index, columns, schema);
        }
    }
    Ok(())
}

pub(crate) fn process_storage_read(
    addr: &H160,
    account_state: &AccountState,
    block_number: &Option<u32>,
    transaction_hash: &Option<Vec<u8>>,
    transaction_index: usize,
    columns: &mut StorageReads,
    schema: &Table,
) {
    if let Some(storage) = &account_state.storage {
        for (slot, value) in storage.iter() {
            columns.n_rows += 1;
            store!(schema, columns, block_number, *block_number);
            store!(schema, columns, transaction_index, Some(transaction_index as u32));
            store!(schema, columns, transaction_hash, transaction_hash.clone());
            store!(schema, columns, contract_address, addr.as_bytes().to_vec());
            store!(schema, columns, slot, slot.as_bytes().to_vec());
            store!(schema, columns, value, value.as_bytes().to_vec());
        }
    }
}
