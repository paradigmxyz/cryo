use std::collections::HashMap;

use ethers::prelude::*;
use polars::prelude::*;
use tokio::sync::mpsc;

use crate::{
    dataframes::SortableDataFrame,
    datasets::state_diffs,
    types::{
        conversions::ToVecHex, BlockChunk, CollectError, ColumnType, Dataset, Datatype, RowFilter,
        Source, Table, ToVecU8, VmTraces,
    },
    with_series, with_series_binary,
};

#[async_trait::async_trait]
impl Dataset for VmTraces {
    fn datatype(&self) -> Datatype {
        Datatype::VmTraces
    }

    fn name(&self) -> &'static str {
        "vm_traces"
    }

    fn column_types(&self) -> HashMap<&'static str, ColumnType> {
        HashMap::from_iter(vec![
            ("block_number", ColumnType::UInt32),
            ("transaction_position", ColumnType::UInt32),
            ("pc", ColumnType::Int64),
            ("cost", ColumnType::Int64),
            ("used", ColumnType::Int64),
            ("push", ColumnType::Binary),
            ("mem_off", ColumnType::Int32),
            ("mem_data", ColumnType::Binary),
            ("storage_key", ColumnType::Binary),
            ("storage_val", ColumnType::Binary),
            ("op", ColumnType::String),
            ("chain_id", ColumnType::Int64),
        ])
    }

    fn default_columns(&self) -> Vec<&'static str> {
        vec!["block_number", "transaction_position", "pc", "cost", "used", "op"]
    }

    fn default_sort(&self) -> Vec<String> {
        vec!["block_number".to_string(), "transaction_position".to_string(), "used".to_string()]
    }

    async fn collect_block_chunk(
        &self,
        chunk: &BlockChunk,
        source: &Source,
        schema: &Table,
        _filter: Option<&RowFilter>,
    ) -> Result<DataFrame, CollectError> {
        let rx = fetch_vm_traces(chunk, source).await;
        vm_traces_to_df(rx, schema, source.chain_id).await
    }
}

async fn fetch_vm_traces(
    block_chunk: &BlockChunk,
    source: &Source,
) -> mpsc::Receiver<(u32, Result<Vec<BlockTrace>, CollectError>)> {
    state_diffs::fetch_block_traces(block_chunk, &[TraceType::VmTrace], source).await
}

struct VmTraceColumns {
    block_number: Vec<u32>,
    transaction_position: Vec<u32>,
    pc: Vec<u64>,
    cost: Vec<u64>,
    used: Vec<Option<u64>>,
    push: Vec<Option<Vec<u8>>>,
    mem_off: Vec<Option<u32>>,
    mem_data: Vec<Option<Vec<u8>>>,
    storage_key: Vec<Option<Vec<u8>>>,
    storage_val: Vec<Option<Vec<u8>>>,
    op: Vec<String>,
    n_rows: usize,
}

async fn vm_traces_to_df(
    mut rx: mpsc::Receiver<(u32, Result<Vec<BlockTrace>, CollectError>)>,
    schema: &Table,
    chain_id: u64,
) -> Result<DataFrame, CollectError> {
    let capacity = 100;
    let mut columns = VmTraceColumns {
        block_number: Vec::with_capacity(capacity),
        transaction_position: Vec::with_capacity(capacity),
        pc: Vec::with_capacity(capacity),
        cost: Vec::with_capacity(capacity),
        used: Vec::with_capacity(capacity),
        push: Vec::with_capacity(capacity),
        mem_off: Vec::with_capacity(capacity),
        mem_data: Vec::with_capacity(capacity),
        storage_key: Vec::with_capacity(capacity),
        storage_val: Vec::with_capacity(capacity),
        op: Vec::with_capacity(capacity),
        n_rows: 0,
    };

    while let Some(message) = rx.recv().await {
        match message {
            (number, Ok(block_traces)) => {
                for (tx_pos, block_trace) in block_traces.into_iter().enumerate() {
                    if let Some(vm_trace) = block_trace.vm_trace {
                        add_ops(vm_trace, schema, &mut columns, number, tx_pos as u32)
                    }
                }
            }
            _ => return Err(CollectError::TooManyRequestsError),
        }
    }

    let mut cols = Vec::new();

    with_series!(cols, "block_number", columns.block_number, schema);
    with_series!(cols, "transaction_position", columns.transaction_position, schema);
    with_series!(cols, "pc", columns.pc, schema);
    with_series!(cols, "cost", columns.cost, schema);
    with_series!(cols, "used", columns.used, schema);
    with_series_binary!(cols, "push", columns.push, schema);
    with_series!(cols, "mem_off", columns.mem_off, schema);
    with_series_binary!(cols, "mem_data", columns.mem_data, schema);
    with_series_binary!(cols, "storage_key", columns.storage_key, schema);
    with_series_binary!(cols, "storage_val", columns.storage_val, schema);
    with_series!(cols, "op", columns.op, schema);

    if schema.has_column("chain_id") {
        cols.push(Series::new("chain_id", vec![chain_id; columns.n_rows]));
    };

    DataFrame::new(cols).map_err(CollectError::PolarsError).sort_by_schema(schema)
}

fn add_ops(
    vm_trace: VMTrace,
    schema: &Table,
    columns: &mut VmTraceColumns,
    number: u32,
    tx_pos: u32,
) {
    for opcode in vm_trace.ops {
        columns.n_rows += 1;

        if schema.has_column("block_number") {
            columns.block_number.push(number);
        };
        if schema.has_column("transaction_position") {
            columns.transaction_position.push(tx_pos);
        };
        if schema.has_column("pc") {
            columns.pc.push(opcode.pc as u64);
        };
        if schema.has_column("cost") {
            columns.cost.push(opcode.cost);
        };

        if let Some(ex) = opcode.ex {
            if schema.has_column("used") {
                columns.used.push(Some(ex.used));
            };
            if schema.has_column("push") {
                columns.push.push(Some(ex.push.to_vec_u8()));
            };
            if let Some(mem) = ex.mem {
                if schema.has_column("mem_off") {
                    columns.mem_off.push(Some(mem.off as u32));
                };
                if schema.has_column("mem_data") {
                    columns.mem_data.push(Some(mem.data.to_vec()));
                };
            } else {
                if schema.has_column("mem_key") {
                    columns.mem_off.push(None);
                };
                if schema.has_column("mem_val") {
                    columns.mem_data.push(None);
                };
            };
            if let Some(store) = ex.store {
                if schema.has_column("storage_key") {
                    columns.storage_key.push(Some(store.key.to_vec_u8()));
                };
                if schema.has_column("storage_val") {
                    columns.storage_val.push(Some(store.val.to_vec_u8()));
                };
            } else {
                if schema.has_column("storage_key") {
                    columns.storage_key.push(None);
                };
                if schema.has_column("storage_val") {
                    columns.storage_val.push(None);
                };
            }
        } else {
            if schema.has_column("used") {
                columns.used.push(None);
            };
            if schema.has_column("push") {
                columns.push.push(None);
            };
            if schema.has_column("mem_key") {
                columns.mem_off.push(None);
            };
            if schema.has_column("mem_val") {
                columns.mem_data.push(None);
            };
            if schema.has_column("storage_key") {
                columns.storage_key.push(None);
            };
            if schema.has_column("storage_val") {
                columns.storage_val.push(None);
            };
        }
        if schema.has_column("op") {
            match opcode.op {
                ExecutedInstruction::Known(op) => columns.op.push(op.to_string()),
                ExecutedInstruction::Unknown(op) => columns.op.push(op),
            }
        };

        if let Some(sub) = opcode.sub {
            add_ops(sub, schema, columns, number, tx_pos)
        }
    }
}
