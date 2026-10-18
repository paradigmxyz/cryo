use crate::*;
use alloy::{primitives::Address, rpc::types::trace::geth::AccountState};
use polars::prelude::*;
use std::collections::BTreeMap;

/// columns for transactions
#[derive(Default, cryo_to_df::ToDataFrames)]
pub struct NonceReads {
    pub(crate) n_rows: u64,
    pub(crate) block_number: Vec<Option<u32>>,
    pub(crate) transaction_index: Vec<Option<u32>>,
    pub(crate) transaction_hash: Vec<Option<RawBytes>>,
    pub(crate) address: Vec<RawBytes>,
    pub(crate) nonce: Vec<u64>,
    pub(crate) chain_id: Vec<u64>,
}

#[async_trait::async_trait]
impl Dataset for NonceReads {}

type BlockTxsTraces = (Option<u32>, Vec<Option<RawBytes>>, Vec<BTreeMap<Address, AccountState>>);

#[async_trait::async_trait]
impl CollectByBlock for NonceReads {
    type Response = BlockTxsTraces;

    async fn extract(request: Params, source: Arc<Source>, query: Arc<Query>) -> R<Self::Response> {
        let schema = query.schemas.get(&Datatype::NonceReads).ok_or(err("schema not provided"))?;
        let include_txs = schema.has_column("transaction_hash");
        source.geth_debug_trace_block_prestate(request.block_number()? as u32, include_txs).await
    }

    fn transform(response: Self::Response, columns: &mut Self, query: &Arc<Query>) -> R<()> {
        process_nonce_reads(&response, columns, &query.schemas)
    }
}

#[async_trait::async_trait]
impl CollectByTransaction for NonceReads {
    type Response = BlockTxsTraces;

    async fn extract(request: Params, source: Arc<Source>, query: Arc<Query>) -> R<Self::Response> {
        let schema = query.schemas.get(&Datatype::NonceReads).ok_or(err("schema not provided"))?;
        let include_block_number = schema.has_column("block_number");
        let tx = request.transaction_hash()?;
        source.geth_debug_trace_transaction_prestate(tx, include_block_number).await
    }

    fn transform(response: Self::Response, columns: &mut Self, query: &Arc<Query>) -> R<()> {
        process_nonce_reads(&response, columns, &query.schemas)
    }
}

pub(crate) fn process_nonce_reads(
    response: &BlockTxsTraces,
    columns: &mut NonceReads,
    schemas: &Schemas,
) -> R<()> {
    let schema = schemas.get(&Datatype::NonceReads).ok_or(err("schema not provided"))?;
    let (block_number, txs, traces) = response;
    for (index, (trace, tx)) in traces.iter().zip(txs).enumerate() {
        for (addr, account_state) in trace.iter() {
            process_nonce_read(addr, account_state, block_number, tx, index, columns, schema);
        }
    }
    Ok(())
}

pub(crate) fn process_nonce_read(
    addr: &Address,
    account_state: &AccountState,
    block_number: &Option<u32>,
    transaction_hash: &Option<RawBytes>,
    transaction_index: usize,
    columns: &mut NonceReads,
    schema: &Table,
) {
    if let Some(nonce) = &account_state.nonce {
        columns.n_rows += 1;
        store!(schema, columns, block_number, *block_number);
        store!(schema, columns, transaction_index, Some(transaction_index as u32));
        store!(schema, columns, transaction_hash, transaction_hash.clone());
        store!(schema, columns, address, addr.to_vec());
        store!(schema, columns, nonce, *nonce);
    }
}
