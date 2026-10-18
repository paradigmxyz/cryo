use crate::*;
use alloy::{
    primitives::{Address, U64},
    rpc::types::trace::parity::{ChangedType, Delta, TraceResults},
};
use polars::prelude::*;

/// columns for transactions
#[derive(Default, cryo_to_df::ToDataFrames)]
pub struct NonceDiffs {
    pub(crate) n_rows: u64,
    pub(crate) block_number: Vec<Option<u32>>,
    pub(crate) transaction_index: Vec<Option<u32>>,
    pub(crate) transaction_hash: Vec<Option<RawBytes>>,
    pub(crate) address: Vec<RawBytes>,
    pub(crate) from_value: Vec<u64>,
    pub(crate) to_value: Vec<u64>,
    pub(crate) chain_id: Vec<u64>,
}

#[async_trait::async_trait]
impl Dataset for NonceDiffs {}

type BlockTxsTraces = (Option<u32>, Vec<Option<RawBytes>>, Vec<TraceResults>);

#[async_trait::async_trait]
impl CollectByBlock for NonceDiffs {
    type Response = BlockTxsTraces;

    async fn extract(request: Params, source: Arc<Source>, query: Arc<Query>) -> R<Self::Response> {
        let schema = query.schemas.get_schema(&Datatype::NonceDiffs)?;
        let include_txs = schema.has_column("transaction_hash");
        let (block_number, txs, traces) =
            source.trace_block_state_diffs(request.block_number()? as u32, include_txs).await?;
        Ok((block_number, txs, traces.into_iter().map(|t| t.full_trace).collect()))
    }

    fn transform(response: Self::Response, columns: &mut Self, query: &Arc<Query>) -> R<()> {
        process_nonce_diffs(&response, columns, &query.schemas)
    }
}

#[async_trait::async_trait]
impl CollectByTransaction for NonceDiffs {
    type Response = BlockTxsTraces;

    async fn extract(request: Params, source: Arc<Source>, _: Arc<Query>) -> R<Self::Response> {
        source.trace_transaction_state_diffs(request.transaction_hash()?).await
    }

    fn transform(response: Self::Response, columns: &mut Self, query: &Arc<Query>) -> R<()> {
        process_nonce_diffs(&response, columns, &query.schemas)
    }
}

pub(crate) fn process_nonce_diffs(
    response: &BlockTxsTraces,
    columns: &mut NonceDiffs,
    schemas: &Schemas,
) -> R<()> {
    let schema = schemas.get(&Datatype::NonceDiffs).ok_or(err("schema not provided"))?;
    let (block_number, txs, traces) = response;
    for (index, (trace, tx)) in traces.iter().zip(txs).enumerate() {
        if let Some(state_diffs) = &trace.state_diff {
            for (addr, diff) in state_diffs.iter() {
                process_nonce_diff(addr, &diff.nonce, block_number, tx, index, columns, schema);
            }
        }
    }
    Ok(())
}

pub(crate) fn process_nonce_diff(
    addr: &Address,
    diff: &Delta<U64>,
    block_number: &Option<u32>,
    transaction_hash: &Option<RawBytes>,
    transaction_index: usize,
    columns: &mut NonceDiffs,
    schema: &Table,
) {
    let (from, to) = match diff {
        Delta::Unchanged => return,
        Delta::Added(value) => (U64::ZERO, *value),
        Delta::Removed(value) => (*value, U64::ZERO),
        Delta::Changed(ChangedType { from, to }) => (*from, *to),
    };
    columns.n_rows += 1;
    store!(schema, columns, block_number, *block_number);
    store!(schema, columns, transaction_index, Some(transaction_index as u32));
    store!(schema, columns, transaction_hash, transaction_hash.clone());
    store!(schema, columns, address, addr.to_vec());
    store!(schema, columns, from_value, from.wrapping_to::<u64>());
    store!(schema, columns, to_value, to.wrapping_to::<u64>());
}
