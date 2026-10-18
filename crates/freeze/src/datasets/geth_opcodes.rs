use crate::*;
use alloy::rpc::types::trace::geth::{
    DefaultFrame, GethDebugTracingOptions, GethDefaultTracingOptions,
};
use polars::prelude::*;

/// columns for geth traces
#[derive(Default, cryo_to_df::ToDataFrames)]
pub struct GethOpcodes {
    n_rows: u64,
    block_number: Vec<Option<u32>>,
    transaction_hash: Vec<Option<RawBytes>>,
    transaction_index: Vec<u32>,
    trace_address: Vec<String>,
    depth: Vec<u64>,
    error: Vec<Option<String>>,
    gas: Vec<u64>,
    gas_cost: Vec<u64>,
    op: Vec<String>,
    pc: Vec<u64>,
    refund_counter: Vec<Option<u64>>,

    memory: Vec<Option<String>>,
    stack: Vec<Option<String>>,
    storage: Vec<Option<String>>,
    return_data: Vec<Option<RawBytes>>,
    chain_id: Vec<u64>,
}

#[async_trait::async_trait]
impl Dataset for GethOpcodes {
    fn default_columns() -> Option<Vec<&'static str>> {
        let f = |x: &&str| x != &"memory" && x != &"stack" && x != &"storage";
        Some(GethOpcodes::column_types().into_keys().filter(f).collect())
    }
}

#[async_trait::async_trait]
impl CollectByBlock for GethOpcodes {
    type Response = (Option<u32>, Vec<Option<RawBytes>>, Vec<DefaultFrame>);

    async fn extract(request: Params, source: Arc<Source>, query: Arc<Query>) -> R<Self::Response> {
        let schema = query.schemas.get_schema(&Datatype::GethOpcodes)?;
        // let options = GethDebugTracingOptions {
        //     disable_storage: Some(!schema.has_column("storage")),
        //     disable_stack: Some(!schema.has_column("stack")),
        //     enable_memory: Some(schema.has_column("memory")),
        //     enable_return_data: Some(schema.has_column("return_data")),
        //     ..Default::default()
        // };
        let config = GethDefaultTracingOptions {
            disable_storage: Some(!schema.has_column("storage")),
            disable_stack: Some(!schema.has_column("stack")),
            enable_memory: Some(schema.has_column("memory")),
            enable_return_data: Some(schema.has_column("return_data")),
            ..Default::default()
        };
        let options = GethDebugTracingOptions { config, ..Default::default() };
        let include_transaction = schema.has_column("block_number");
        let block_number = request.block_number()? as u32;
        source.geth_debug_trace_block_opcodes(block_number, include_transaction, options).await
    }

    fn transform(response: Self::Response, columns: &mut Self, query: &Arc<Query>) -> R<()> {
        process_geth_opcodes(response, columns, &query.schemas)
    }
}

#[async_trait::async_trait]
impl CollectByTransaction for GethOpcodes {
    type Response = (Option<u32>, Vec<Option<RawBytes>>, Vec<DefaultFrame>);

    async fn extract(request: Params, source: Arc<Source>, query: Arc<Query>) -> R<Self::Response> {
        let schema = query.schemas.get_schema(&Datatype::GethOpcodes)?;
        let include_block_number = schema.has_column("block_number");
        let config = GethDefaultTracingOptions {
            disable_storage: Some(!schema.has_column("storage")),
            disable_stack: Some(!schema.has_column("stack")),
            enable_memory: Some(schema.has_column("memory")),
            enable_return_data: Some(schema.has_column("return_data")),
            ..Default::default()
        };
        let options = GethDebugTracingOptions { config, ..Default::default() };
        source
            .geth_debug_trace_transaction_opcodes(
                request.transaction_hash()?,
                include_block_number,
                options,
            )
            .await
    }

    fn transform(response: Self::Response, columns: &mut Self, query: &Arc<Query>) -> R<()> {
        process_geth_opcodes(response, columns, &query.schemas)
    }
}

fn process_geth_opcodes(
    traces: (Option<u32>, Vec<Option<RawBytes>>, Vec<DefaultFrame>),
    columns: &mut GethOpcodes,
    schemas: &Schemas,
) -> R<()> {
    let (block_number, txs, traces) = traces;
    let schema =
        schemas.get(&Datatype::GethOpcodes).ok_or(err("schema for geth_traces missing"))?;
    for (tx_index, (tx, trace)) in txs.into_iter().zip(traces).enumerate() {
        process_trace(trace, columns, schema, &block_number, &tx, tx_index as u32, vec![])?
    }
    Ok(())
}

fn process_trace(
    trace: DefaultFrame,
    columns: &mut GethOpcodes,
    schema: &Table,
    block_number: &Option<u32>,
    tx: &Option<RawBytes>,
    tx_index: u32,
    trace_address: Vec<u32>,
) -> R<()> {
    let n_struct_logs = trace.struct_logs.len();
    for (i, struct_log) in trace.struct_logs.into_iter().enumerate() {
        columns.n_rows += 1;

        store!(schema, columns, block_number, *block_number);
        store!(schema, columns, transaction_hash, tx.clone());
        store!(schema, columns, transaction_index, tx_index);
        store!(
            schema,
            columns,
            trace_address,
            trace_address.iter().map(|&n| n.to_string()).collect::<Vec<_>>().join(" ")
        );

        store!(schema, columns, depth, struct_log.depth);
        store!(schema, columns, error, struct_log.error);
        store!(schema, columns, gas, struct_log.gas);
        store!(schema, columns, gas_cost, struct_log.gas_cost);
        store!(schema, columns, pc, struct_log.pc);
        store!(schema, columns, op, struct_log.op.to_string());
        store!(schema, columns, refund_counter, struct_log.refund_counter);

        if schema.has_column("memory") {
            let memory_str = serde_json::to_string(&struct_log.memory)
                .map_err(|_| err("could not encode opcode memory"))?;
            store!(schema, columns, memory, Some(memory_str));
        }
        if schema.has_column("stack") {
            let stack_str = serde_json::to_string(&struct_log.stack)
                .map_err(|_| err("could not encode opcode stack"))?;
            store!(schema, columns, stack, Some(stack_str));
        }
        if schema.has_column("storage") {
            let storage_str = serde_json::to_string(&struct_log.storage)
                .map_err(|_| err("could not encode opcode storage"))?;
            store!(schema, columns, storage, Some(storage_str));
        }

        if i == n_struct_logs - 1 {
            store!(schema, columns, return_data, Some(trace.return_value.to_vec()));
        } else {
            store!(schema, columns, return_data, None);
        }
    }
    Ok(())
}
