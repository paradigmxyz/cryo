use crate::*;
use alloy::{primitives::U256, rpc::types::trace::geth::CallFrame};
use polars::prelude::*;

/// columns for geth traces
#[derive(Default, cryo_to_df::ToDataFrames)]
pub struct GethCalls {
    n_rows: u64,
    typ: Vec<String>,
    from_address: Vec<RawBytes>,
    to_address: Vec<Option<RawBytes>>,
    value: Vec<Option<U256>>,
    gas: Vec<U256>,
    gas_used: Vec<U256>,
    input: Vec<RawBytes>,
    output: Vec<Option<RawBytes>>,
    error: Vec<Option<String>>,
    block_number: Vec<Option<u32>>,
    transaction_hash: Vec<Option<RawBytes>>,
    transaction_index: Vec<u32>,
    trace_address: Vec<String>,
    chain_id: Vec<u64>,
}

#[async_trait::async_trait]
impl Dataset for GethCalls {}

#[async_trait::async_trait]
impl CollectByBlock for GethCalls {
    type Response = (Option<u32>, Vec<Option<RawBytes>>, Vec<CallFrame>);

    async fn extract(request: Params, source: Arc<Source>, query: Arc<Query>) -> R<Self::Response> {
        let schema = query.schemas.get_schema(&Datatype::GethCalls)?;
        let include_transaction = schema.has_column("block_number");
        let block_number = request.block_number()? as u32;
        source.geth_debug_trace_block_calls(block_number, include_transaction).await
    }

    fn transform(response: Self::Response, columns: &mut Self, query: &Arc<Query>) -> R<()> {
        process_geth_traces(response, columns, &query.schemas)
    }
}

#[async_trait::async_trait]
impl CollectByTransaction for GethCalls {
    type Response = (Option<u32>, Vec<Option<RawBytes>>, Vec<CallFrame>);

    async fn extract(request: Params, source: Arc<Source>, query: Arc<Query>) -> R<Self::Response> {
        let schema = query.schemas.get_schema(&Datatype::GethCalls)?;
        let include_block_number = schema.has_column("block_number");
        source
            .geth_debug_trace_transaction_calls(request.transaction_hash()?, include_block_number)
            .await
    }

    fn transform(response: Self::Response, columns: &mut Self, query: &Arc<Query>) -> R<()> {
        process_geth_traces(response, columns, &query.schemas)
    }
}

fn process_geth_traces(
    traces: (Option<u32>, Vec<Option<RawBytes>>, Vec<CallFrame>),
    columns: &mut GethCalls,
    schemas: &Schemas,
) -> R<()> {
    let (block_number, txs, traces) = traces;
    let schema = schemas.get(&Datatype::GethCalls).ok_or(err("schema for geth_traces missing"))?;
    for (tx_index, (tx, trace)) in txs.into_iter().zip(traces).enumerate() {
        process_trace(trace, columns, schema, &block_number, &tx, tx_index as u32, vec![])?
    }
    Ok(())
}

fn process_trace(
    trace: CallFrame,
    columns: &mut GethCalls,
    schema: &Table,
    block_number: &Option<u32>,
    tx: &Option<RawBytes>,
    tx_index: u32,
    trace_address: Vec<u32>,
) -> R<()> {
    columns.n_rows += 1;
    store!(schema, columns, typ, trace.typ);
    store!(schema, columns, from_address, trace.from.to_vec());
    store!(schema, columns, to_address, trace.to.map(|x| x.to_vec()));
    store!(schema, columns, value, trace.value);
    store!(schema, columns, gas, trace.gas);
    store!(schema, columns, gas_used, trace.gas_used);
    store!(schema, columns, input, trace.input.0.to_vec());
    store!(schema, columns, output, trace.output.map(|x| x.0.to_vec()));
    store!(schema, columns, error, trace.error);
    store!(schema, columns, block_number, *block_number);
    store!(schema, columns, transaction_hash, tx.clone());
    store!(schema, columns, transaction_index, tx_index);
    store!(
        schema,
        columns,
        trace_address,
        trace_address.iter().map(|&n| n.to_string()).collect::<Vec<_>>().join(" ")
    );

    for (s, subcall) in trace.calls.into_iter().enumerate() {
        let mut sub_trace_address = trace_address.clone();
        sub_trace_address.push(s as u32);
        process_trace(subcall, columns, schema, block_number, tx, tx_index, sub_trace_address)?
    }

    Ok(())
}
