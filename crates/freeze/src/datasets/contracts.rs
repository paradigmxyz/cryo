use std::collections::HashMap;

use ethers::prelude::*;
use polars::prelude::*;
use tokio::sync::mpsc;

use super::traces;
use crate::{
    types::{
        conversions::ToVecHex, dataframes::SortableDataFrame, BlockChunk, CollectError, ColumnType,
        Contracts, Dataset, Datatype, RowFilter, Source, Table, TransactionChunk,
    },
    with_series, with_series_binary,
};

#[async_trait::async_trait]
impl Dataset for Contracts {
    fn datatype(&self) -> Datatype {
        Datatype::Contracts
    }

    fn name(&self) -> &'static str {
        "contracts"
    }

    fn column_types(&self) -> HashMap<&'static str, ColumnType> {
        HashMap::from_iter(vec![
            ("block_number", ColumnType::UInt32),
            ("create_index", ColumnType::UInt32),
            ("transaction_hash", ColumnType::Binary),
            ("contract_address", ColumnType::Binary),
            ("deployer", ColumnType::Binary),
            ("factory", ColumnType::Binary),
            ("init_code", ColumnType::Binary),
            ("code", ColumnType::Binary),
            ("init_code_hash", ColumnType::Binary),
            ("code_hash", ColumnType::Binary),
            ("chain_id", ColumnType::UInt64),
        ])
    }

    fn default_columns(&self) -> Vec<&'static str> {
        vec![
            "block_number",
            "create_index",
            "transaction_hash",
            "contract_address",
            "deployer",
            "factory",
            "init_code",
            "code",
            "init_code_hash",
            "code_hash",
        ]
    }

    fn default_sort(&self) -> Vec<String> {
        vec!["block_number".to_string(), "create_index".to_string()]
    }

    async fn collect_block_chunk(
        &self,
        chunk: &BlockChunk,
        source: &Source,
        schema: &Table,
        _filter: Option<&RowFilter>,
    ) -> Result<DataFrame, CollectError> {
        let rx = traces::fetch_block_traces(chunk, source);
        traces_to_contracts_df(rx, schema, source.chain_id).await
    }

    async fn collect_transaction_chunk(
        &self,
        chunk: &TransactionChunk,
        source: &Source,
        schema: &Table,
        _filter: Option<&RowFilter>,
    ) -> Result<DataFrame, CollectError> {
        let rx = traces::fetch_transaction_traces(chunk, source);
        traces_to_contracts_df(rx, schema, source.chain_id).await
    }
}

struct ContractsColumns {
    block_number: Vec<u32>,
    create_index: Vec<u32>,
    transaction_hash: Vec<Option<Vec<u8>>>,
    contract_address: Vec<Vec<u8>>,
    deployer: Vec<Vec<u8>>,
    factory: Vec<Vec<u8>>,
    init_code: Vec<Vec<u8>>,
    code: Vec<Vec<u8>>,
    init_code_hash: Vec<Vec<u8>>,
    code_hash: Vec<Vec<u8>>,
    chain_id: Vec<u64>,
    n_rows: usize,
}

impl ContractsColumns {
    fn new(capacity: usize) -> ContractsColumns {
        ContractsColumns {
            block_number: Vec::with_capacity(capacity),
            create_index: Vec::with_capacity(capacity),
            transaction_hash: Vec::with_capacity(capacity),
            contract_address: Vec::with_capacity(capacity),
            deployer: Vec::with_capacity(capacity),
            factory: Vec::with_capacity(capacity),
            init_code: Vec::with_capacity(capacity),
            code: Vec::with_capacity(capacity),
            init_code_hash: Vec::with_capacity(capacity),
            code_hash: Vec::with_capacity(capacity),
            chain_id: Vec::with_capacity(capacity),
            n_rows: 0,
        }
    }

    fn into_df(self, schema: &Table, chain_id: u64) -> Result<DataFrame, CollectError> {
        let mut cols = Vec::new();

        with_series!(cols, "block_number", self.block_number, schema);
        with_series!(cols, "create_index", self.create_index, schema);
        with_series_binary!(cols, "transaction_hash", self.transaction_hash, schema);
        with_series_binary!(cols, "contract_address", self.contract_address, schema);
        with_series_binary!(cols, "deployer", self.deployer, schema);
        with_series_binary!(cols, "factory", self.factory, schema);
        with_series_binary!(cols, "init_code", self.init_code, schema);
        with_series_binary!(cols, "code", self.code, schema);
        with_series_binary!(cols, "init_code_hash", self.init_code_hash, schema);
        with_series_binary!(cols, "code_hash", self.code_hash, schema);
        with_series!(cols, "chain_id", self.chain_id, schema);

        if schema.has_column("chain_id") {
            cols.push(Series::new("chain_id", vec![chain_id; self.n_rows]));
        };

        DataFrame::new(cols).map_err(CollectError::PolarsError).sort_by_schema(schema)
    }
}

async fn traces_to_contracts_df(
    mut rx: mpsc::Receiver<Result<Vec<Trace>, CollectError>>,
    schema: &Table,
    chain_id: u64,
) -> Result<DataFrame, CollectError> {
    let mut columns = ContractsColumns::new(200);
    while let Some(message) = rx.recv().await {
        match message {
            Ok(traces) => process_traces(traces, schema, &mut columns)?,
            _ => return Err(CollectError::TooManyRequestsError),
        }
    }
    columns.into_df(schema, chain_id)
}

fn process_traces(
    traces: Vec<Trace>,
    schema: &Table,
    columns: &mut ContractsColumns,
) -> Result<(), CollectError> {
    let traces = filter_failed_traces(traces);

    let mut deployer = H160([0; 20]);
    let mut create_index = 0;
    for trace in traces.iter() {
        if trace.trace_address.is_empty() {
            deployer = match &trace.action {
                Action::Call(call) => call.from,
                Action::Create(create) => create.from,
                Action::Suicide(suicide) => suicide.refund_address,
                Action::Reward(reward) => reward.author,
            };
        };

        if let (Action::Create(create), Some(Res::Create(result))) = (&trace.action, &trace.result)
        {
            columns.n_rows += 1;
            if schema.has_column("block_number") {
                columns.block_number.push(trace.block_number as u32);
            }
            if schema.has_column("create_index") {
                columns.create_index.push(create_index);
                create_index += 1;
            }
            if schema.has_column("transaction_hash") {
                match trace.transaction_hash {
                    Some(tx_hash) => columns.transaction_hash.push(Some(tx_hash.as_bytes().into())),
                    None => columns.transaction_hash.push(None),
                }
            }
            if schema.has_column("contract_address") {
                columns.contract_address.push(result.address.as_bytes().into())
            }
            if schema.has_column("deployer") {
                columns.deployer.push(deployer.as_bytes().into())
            }
            if schema.has_column("factory") {
                columns.factory.push(create.from.as_bytes().into())
            }
            if schema.has_column("init_code") {
                columns.init_code.push(create.init.to_vec())
            }
            if schema.has_column("code") {
                columns.code.push(result.code.to_vec())
            }
            if schema.has_column("init_code_hash") {
                columns
                    .init_code_hash
                    .push(ethers_core::utils::keccak256(create.init.clone()).into())
            }
            if schema.has_column("code_hash") {
                columns.code_hash.push(ethers_core::utils::keccak256(result.code.clone()).into())
            }
        }
    }
    Ok(())
}

/// filter out error traces
fn filter_failed_traces(traces: Vec<Trace>) -> Vec<Trace> {
    let mut error_address: Option<Vec<usize>> = None;
    let mut filtered: Vec<Trace> = Vec::new();

    for trace in traces.into_iter() {
        // restart for each transaction
        if trace.trace_address.is_empty() {
            error_address = None;
        };

        // if in an error, check if next trace is still in error
        if let Some(ref e_address) = error_address {
            if trace.trace_address.len() >= e_address.len() &&
                trace.trace_address[0..e_address.len()] == e_address[..]
            {
                continue
            } else {
                error_address = None;
            }
        }

        // check if current trace is start of an error
        match trace.error {
            Some(_) => error_address = Some(trace.trace_address),
            None => filtered.push(trace),
        }
    }

    filtered
}
