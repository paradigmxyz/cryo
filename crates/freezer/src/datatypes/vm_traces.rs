use std::collections::HashMap;

use ethers::prelude::*;
use polars::prelude::*;

use crate::fetch;
use crate::types::BlockChunk;
use crate::types::CollectError;
use crate::types::ColumnType;
use crate::types::Dataset;
use crate::types::Datatype;
use crate::types::FreezeOpts;
use crate::types::Schema;
use crate::types::VmTraces;
use crate::types::ToVecU8;

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
            ("pc", ColumnType::Int64),
            ("cost", ColumnType::Int64),
            ("used", ColumnType::Int64),
            ("push", ColumnType::Binary),
            ("mem_off", ColumnType::Int32),
            ("mem_data", ColumnType::Binary),
            ("storage_key", ColumnType::Binary),
            ("storage_val", ColumnType::Binary),
            ("op", ColumnType::String),
        ])
    }

    fn default_columns(&self) -> Vec<&'static str> {
        vec!["pc", "cost", "used", "op"]
    }

    fn default_sort(&self) -> Vec<String> {
        vec![]
    }

    async fn collect_chunk(
        &self,
        block_chunk: &BlockChunk,
        opts: &FreezeOpts,
    ) -> Result<DataFrame, CollectError> {
        let vm_traces =
            fetch::fetch_vm_traces(block_chunk, &opts.provider, &opts.max_concurrent_blocks)
                .await?;
        vm_traces_to_df(vm_traces, &opts.schemas[&Datatype::VmTraces])
            .map_err(CollectError::PolarsError)
    }
}

pub fn vm_traces_to_df(
    block_traces: Vec<BlockTrace>,
    schema: &Schema,
) -> Result<DataFrame, PolarsError> {
    let mut columns = VmTraceColumns {
        pc: Vec::with_capacity(block_traces.len()),
        cost: Vec::with_capacity(block_traces.len()),
        used: Vec::with_capacity(block_traces.len()),
        push: Vec::with_capacity(block_traces.len()),
        mem_off: Vec::with_capacity(block_traces.len()),
        mem_data: Vec::with_capacity(block_traces.len()),
        storage_key: Vec::with_capacity(block_traces.len()),
        storage_val: Vec::with_capacity(block_traces.len()),
        op: Vec::with_capacity(block_traces.len()),
    };

    for block_trace in block_traces.into_iter() {
        if let Some(vm_trace) = block_trace.vm_trace {
            add_ops(vm_trace, schema, &mut columns)
        }
    }

    let mut series = Vec::new();
    if schema.contains_key("pc") {
        series.push(Series::new("pc", columns.pc));
    };
    if schema.contains_key("cost") {
        series.push(Series::new("cost", columns.cost));
    };
    if schema.contains_key("used") {
        series.push(Series::new("used", columns.used));
    };
    if schema.contains_key("push") {
        series.push(Series::new("push", columns.push));
    };
    if schema.contains_key("mem_off") {
        series.push(Series::new("mem_off", columns.mem_off));
    };
    if schema.contains_key("mem_data") {
        series.push(Series::new("mem_data", columns.mem_data));
    };
    if schema.contains_key("storage_key") {
        series.push(Series::new("storage_key", columns.storage_key));
    };
    if schema.contains_key("storage_val") {
        series.push(Series::new("storage_val", columns.storage_val));
    };
    if schema.contains_key("op") {
        series.push(Series::new("op", columns.op));
    };
    DataFrame::new(series)
}

struct VmTraceColumns {
    pc: Vec<u64>,
    cost: Vec<u64>,
    used: Vec<Option<u64>>,
    push: Vec<Option<Vec<u8>>>,
    mem_off: Vec<Option<u32>>,
    mem_data: Vec<Option<Vec<u8>>>,
    storage_key: Vec<Option<Vec<u8>>>,
    storage_val: Vec<Option<Vec<u8>>>,
    op: Vec<String>,
}

fn add_ops(vm_trace: VMTrace, schema: &Schema, columns: &mut VmTraceColumns) {
    for opcode in vm_trace.ops {

        if schema.contains_key("pc") {
            columns.pc.push(opcode.pc as u64);
        };
        if schema.contains_key("cost") {
            columns.cost.push(opcode.cost);
        };

        if let Some(ex) = opcode.ex {
            if schema.contains_key("used") {
                columns.used.push(Some(ex.used));
            };
            if schema.contains_key("push") {
                columns.push.push(Some(ex.push.to_vec_u8()));
            };
            if let Some(mem) = ex.mem {
                if schema.contains_key("mem_off") {
                    columns.mem_off.push(Some(mem.off as u32));
                };
                if schema.contains_key("mem_data") {
                    columns.mem_data.push(Some(mem.data.to_vec()));
                };
            } else {
                if schema.contains_key("mem_key") {
                    columns.mem_off.push(None);
                };
                if schema.contains_key("mem_val") {
                    columns.mem_data.push(None);
                };
            };
            if let Some(store) = ex.store {
                if schema.contains_key("storage_key") {
                    columns.storage_key.push(Some(store.key.to_vec_u8()));
                };
                if schema.contains_key("storage_val") {
                    columns.storage_val.push(Some(store.val.to_vec_u8()));
                };
            } else {
                if schema.contains_key("storage_key") {
                    columns.storage_key.push(None);
                };
                if schema.contains_key("storage_val") {
                    columns.storage_val.push(None);
                };
            }
        } else {
            if schema.contains_key("used") {
                columns.used.push(None);
            };
            if schema.contains_key("push") {
                columns.push.push(None);
            };
            if schema.contains_key("mem_key") {
                columns.mem_off.push(None);
            };
            if schema.contains_key("mem_val") {
                columns.mem_data.push(None);
            };
            if schema.contains_key("storage_key") {
                columns.storage_key.push(None);
            };
            if schema.contains_key("storage_val") {
                columns.storage_val.push(None);
            };
        }
        if schema.contains_key("op") {
            match opcode.op {
                ExecutedInstruction::Known(op) => columns.op.push(op.to_string()),
                ExecutedInstruction::Unknown(op) => columns.op.push(op),
            }
        };

        if let Some(sub) = opcode.sub {
            add_ops(sub, schema, columns)
        }
    }
}

