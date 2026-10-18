use crate::*;
use alloy::{
    primitives::U256,
    rpc::types::trace::parity::{Action, LocalizedTransactionTrace, TraceOutput},
};
use polars::prelude::*;

/// columns for transactions
#[derive(Default, cryo_to_df::ToDataFrames)]
pub struct NativeTransfers {
    n_rows: u64,
    block_number: Vec<u32>,
    block_hash: Vec<RawBytes>,
    transaction_index: Vec<Option<u32>>,
    transfer_index: Vec<u32>,
    transaction_hash: Vec<Option<RawBytes>>,
    from_address: Vec<RawBytes>,
    to_address: Vec<RawBytes>,
    value: Vec<U256>,
    chain_id: Vec<u64>,
}

#[async_trait::async_trait]
impl Dataset for NativeTransfers {
    fn optional_parameters() -> Vec<Dim> {
        vec![Dim::FromAddress, Dim::ToAddress]
    }
}

#[async_trait::async_trait]
impl CollectByBlock for NativeTransfers {
    type Response = Vec<LocalizedTransactionTrace>;

    async fn extract(request: Params, source: Arc<Source>, _: Arc<Query>) -> R<Self::Response> {
        let traces = source.trace_block(request.block_number()?).await?;
        Ok(filter_traces_by_from_to_addresses(traces, &request.from_address, &request.to_address))
    }

    fn transform(response: Self::Response, columns: &mut Self, query: &Arc<Query>) -> R<()> {
        let traces =
            if query.exclude_failed { traces::filter_failed_traces(response) } else { response };
        process_native_transfers(&traces, columns, &query.schemas)
    }
}

#[async_trait::async_trait]
impl CollectByTransaction for NativeTransfers {
    type Response = Vec<LocalizedTransactionTrace>;

    async fn extract(request: Params, source: Arc<Source>, _: Arc<Query>) -> R<Self::Response> {
        let traces = source.trace_transaction(request.ethers_transaction_hash()?).await?;
        Ok(filter_traces_by_from_to_addresses(traces, &request.from_address, &request.to_address))
    }

    fn transform(response: Self::Response, columns: &mut Self, query: &Arc<Query>) -> R<()> {
        let traces =
            if query.exclude_failed { traces::filter_failed_traces(response) } else { response };
        process_native_transfers(&traces, columns, &query.schemas)
    }
}

/// process block into columns
pub(crate) fn process_native_transfers(
    traces: &[LocalizedTransactionTrace],
    columns: &mut NativeTransfers,
    schemas: &Schemas,
) -> R<()> {
    let schema = schemas.get(&Datatype::NativeTransfers).ok_or(err("schema not provided"))?;
    for (transfer_index, trace) in traces.iter().enumerate() {
        columns.n_rows += 1;
        store!(schema, columns, block_number, trace.block_number.unwrap_or(0) as u32);
        store!(schema, columns, transaction_index, trace.transaction_position.map(|x| x as u32));
        store!(schema, columns, block_hash, trace.block_hash.unwrap().to_vec());
        store!(schema, columns, transfer_index, transfer_index as u32);
        store!(schema, columns, transaction_hash, trace.transaction_hash.map(|x| x.to_vec()));

        match &trace.trace.action {
            Action::Call(action) => {
                store!(schema, columns, from_address, action.from.to_vec());
                store!(schema, columns, to_address, action.to.to_vec());
                store!(schema, columns, value, action.value);
            }
            Action::Create(action) => {
                store!(schema, columns, from_address, action.from.to_vec());
                match &trace.trace.result.as_ref() {
                    Some(TraceOutput::Create(res)) => {
                        store!(schema, columns, to_address, res.address.0.to_vec())
                    }
                    _ => store!(schema, columns, to_address, vec![0; 32]),
                }
                store!(schema, columns, value, action.value);
            }
            Action::Selfdestruct(action) => {
                store!(schema, columns, from_address, action.address.to_vec());
                store!(schema, columns, to_address, action.refund_address.to_vec());
                store!(schema, columns, value, action.balance);
            }
            Action::Reward(action) => {
                store!(schema, columns, from_address, vec![0; 20]);
                store!(schema, columns, to_address, action.author.to_vec());
                store!(schema, columns, value, action.value);
            }
        }
    }
    Ok(())
}
