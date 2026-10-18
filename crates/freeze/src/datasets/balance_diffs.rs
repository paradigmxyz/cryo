use crate::*;
use alloy::{
    primitives::{Address, U256},
    rpc::types::trace::parity::{ChangedType, Delta, TraceResults},
};
use polars::prelude::*;

/// columns for transactions
#[derive(Default, cryo_to_df::ToDataFrames)]
pub struct BalanceDiffs {
    pub(crate) n_rows: u64,
    pub(crate) block_number: Vec<Option<u32>>,
    pub(crate) transaction_index: Vec<Option<u32>>,
    pub(crate) transaction_hash: Vec<Option<RawBytes>>,
    pub(crate) address: Vec<RawBytes>,
    pub(crate) from_value: Vec<U256>,
    pub(crate) to_value: Vec<U256>,
    pub(crate) chain_id: Vec<u64>,
}

#[async_trait::async_trait]
impl Dataset for BalanceDiffs {}

type BlockTxsTraces = (Option<u32>, Vec<Option<RawBytes>>, Vec<TraceResults>);

#[async_trait::async_trait]
impl CollectByBlock for BalanceDiffs {
    type Response = BlockTxsTraces;

    async fn extract(request: Params, source: Arc<Source>, query: Arc<Query>) -> R<Self::Response> {
        let schema =
            query.schemas.get(&Datatype::BalanceDiffs).ok_or(err("schema not provided"))?;
        let include_txs = schema.has_column("transaction_hash");
        let (bn, txs, traces) =
            source.trace_block_state_diffs(request.block_number()? as u32, include_txs).await?;
        let trace_results = traces.into_iter().map(|t| t.full_trace).collect();
        Ok((bn, txs, trace_results))
    }

    fn transform(response: Self::Response, columns: &mut Self, query: &Arc<Query>) -> R<()> {
        process_balance_diffs(&response, columns, &query.schemas)
    }
}

#[async_trait::async_trait]
impl CollectByTransaction for BalanceDiffs {
    type Response = BlockTxsTraces;

    async fn extract(request: Params, source: Arc<Source>, _: Arc<Query>) -> R<Self::Response> {
        source.trace_transaction_state_diffs(request.transaction_hash()?).await
    }

    fn transform(response: Self::Response, columns: &mut Self, query: &Arc<Query>) -> R<()> {
        process_balance_diffs(&response, columns, &query.schemas)
    }
}

pub(crate) fn process_balance_diffs(
    response: &BlockTxsTraces,
    columns: &mut BalanceDiffs,
    schemas: &Schemas,
) -> R<()> {
    let schema = schemas.get(&Datatype::BalanceDiffs).ok_or(err("schema not provided"))?;
    let (block_number, txs, traces) = response;
    for (index, (trace, tx)) in traces.iter().zip(txs).enumerate() {
        if let Some(state_diffs) = &trace.state_diff {
            for (addr, diff) in state_diffs.iter() {
                process_balance_diff(addr, &diff.balance, block_number, tx, index, columns, schema);
            }
        }
    }
    Ok(())
}

pub(crate) fn process_balance_diff(
    addr: &Address,
    diff: &Delta<U256>,
    block_number: &Option<u32>,
    transaction_hash: &Option<RawBytes>,
    transaction_index: usize,
    columns: &mut BalanceDiffs,
    schema: &Table,
) {
    let (from, to) = match diff {
        Delta::Unchanged => return,
        Delta::Added(value) => (U256::ZERO, *value),
        Delta::Removed(value) => (*value, U256::ZERO),
        Delta::Changed(ChangedType { from, to }) => (*from, *to),
    };
    columns.n_rows += 1;
    store!(schema, columns, block_number, *block_number);
    store!(schema, columns, transaction_index, Some(transaction_index as u32));
    store!(schema, columns, transaction_hash, transaction_hash.clone());
    store!(schema, columns, address, addr.to_vec());
    store!(schema, columns, from_value, from);
    store!(schema, columns, to_value, to);
}
