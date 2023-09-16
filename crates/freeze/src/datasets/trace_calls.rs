// want to be able to include either the raw call data or the decoded arguments
// want to be able to include raw_output or decoded columns
// - could do output_decoded as a json object

use crate::{types::TraceCalls, ColumnType, Dataset, Datatype, Traces, U256Type};
use std::collections::HashMap;

use crate::{conversions::ToVecHex, types::conversions::ToVecU8};
use tokio::{sync::mpsc, task};

use ethers::prelude::*;
use polars::prelude::*;

use super::traces;
use crate::{
    dataframes::SortableDataFrame,
    types::{AddressChunk, BlockChunk, CallDataChunk, CollectError, RowFilter, Source, Table},
    with_series, with_series_binary, with_series_u256, ColumnEncoding,
};

#[async_trait::async_trait]
impl Dataset for TraceCalls {
    fn datatype(&self) -> Datatype {
        Datatype::TraceCalls
    }

    fn name(&self) -> &'static str {
        "trace_calls"
    }

    fn column_types(&self) -> HashMap<&'static str, ColumnType> {
        let mut types = Traces.column_types();
        types.insert("tx_to_address", ColumnType::Binary);
        types.insert("tx_call_data", ColumnType::Binary);
        types
    }

    fn default_columns(&self) -> Vec<&'static str> {
        Traces.default_columns()
    }

    fn default_sort(&self) -> Vec<String> {
        Traces.default_sort()
    }

    async fn collect_block_chunk(
        &self,
        chunk: &BlockChunk,
        source: &Source,
        schema: &Table,
        filter: Option<&RowFilter>,
    ) -> Result<DataFrame, CollectError> {
        let (address_chunks, call_data_chunks) = match filter {
            Some(filter) => (filter.address_chunks()?, filter.call_data_chunks()?),
            _ => return Err(CollectError::CollectError("must specify RowFilter".to_string())),
        };
        let rx = fetch_trace_calls(vec![chunk], address_chunks, call_data_chunks, source).await;
        trace_calls_to_df(rx, schema, source.chain_id).await
    }
}

// block, address, call_data, BlockTrace
type TraceCallOutput = (u64, Vec<u8>, Vec<u8>, BlockTrace);

async fn fetch_trace_calls(
    block_chunks: Vec<&BlockChunk>,
    address_chunks: Vec<AddressChunk>,
    call_data_chunks: Vec<CallDataChunk>,
    source: &Source,
) -> mpsc::Receiver<Result<TraceCallOutput, CollectError>> {
    let (tx, rx) = mpsc::channel(100);

    for block_chunk in block_chunks {
        for number in block_chunk.numbers() {
            for address_chunk in &address_chunks {
                for address in address_chunk.values().iter() {
                    for call_data_chunk in &call_data_chunks {
                        for call_data in call_data_chunk.values().iter() {
                            let address = address.clone();
                            let address_h160 = H160::from_slice(&address);
                            let call_data = call_data.clone();

                            let tx = tx.clone();
                            let source = source.clone();
                            task::spawn(async move {
                                let transaction = TransactionRequest {
                                    to: Some(address_h160.into()),
                                    data: Some(call_data.clone().into()),
                                    ..Default::default()
                                };
                                // let transaction =
                                // ethers::types::transaction::eip2718::TypedTransaction::Legacy(transaction);
                                // let transaction =
                                // TypedTransaction::Legacy(transaction);
                                let trace_type = vec![TraceType::Trace];

                                let result = source
                                    .fetcher
                                    .trace_call(transaction, trace_type, Some(number.into()))
                                    .await;
                                let result = match result {
                                    Ok(value) => Ok((number, address, call_data, value)),
                                    Err(e) => Err(e),
                                };
                                match tx.send(result).await {
                                    Ok(_) => {}
                                    Err(tokio::sync::mpsc::error::SendError(_e)) => {
                                        eprintln!("send error, try using a rate limit with --requests-per-second or limiting max concurrency with --max-concurrent-requests");
                                        std::process::exit(1)
                                    }
                                }
                            });
                        }
                    }
                }
            }
        }
    }

    rx
}

async fn trace_calls_to_df(
    mut stream: mpsc::Receiver<Result<TraceCallOutput, CollectError>>,
    schema: &Table,
    chain_id: u64,
) -> Result<DataFrame, CollectError> {
    // initialize
    let mut columns = TraceCallColumns::default();

    // parse stream of blocks
    while let Some(message) = stream.recv().await {
        match message {
            Ok(trace_call_output) => {
                columns.process_calls(trace_call_output, schema);
            }
            Err(e) => {
                println!("{:?}", e);
                return Err(CollectError::TooManyRequestsError)
            }
        }
    }

    // convert to dataframes
    columns.create_df(schema, chain_id)
}

#[derive(Default)]
struct TraceCallColumns {
    n_rows: usize,
    tx_to_address: Vec<Vec<u8>>,
    tx_call_data: Vec<Vec<u8>>,
    action_from: Vec<Option<Vec<u8>>>,
    action_to: Vec<Option<Vec<u8>>>,
    action_value: Vec<U256>,
    action_gas: Vec<Option<u32>>,
    action_input: Vec<Option<Vec<u8>>>,
    action_call_type: Vec<Option<String>>,
    action_init: Vec<Option<Vec<u8>>>,
    action_reward_type: Vec<Option<String>>,
    action_type: Vec<String>,
    result_gas_used: Vec<Option<u32>>,
    result_output: Vec<Option<Vec<u8>>>,
    result_code: Vec<Option<Vec<u8>>>,
    result_address: Vec<Option<Vec<u8>>>,
    trace_address: Vec<String>,
    subtraces: Vec<u32>,
    transaction_position: Vec<u32>,
    transaction_hash: Vec<Option<Vec<u8>>>,
    block_number: Vec<u32>,
    block_hash: Vec<Option<Vec<u8>>>,
    error: Vec<Option<String>>,
}

impl TraceCallColumns {
    fn process_calls(&mut self, trace_call_output: TraceCallOutput, schema: &Table) {
        let (block_number, contract_address, call_data, output_data) = trace_call_output;
        if let Some(tx_traces) = output_data.trace {
            for tx_trace in tx_traces.iter() {
                self.n_rows += 1;

                if schema.has_column("tx_to_address") {
                    self.tx_to_address.push(contract_address.clone());
                }
                if schema.has_column("tx_call_data") {
                    self.tx_call_data.push(call_data.clone());
                }

                match &tx_trace.action {
                    Action::Call(a) => {
                        if schema.has_column("action_from") {
                            self.action_from.push(Some(a.from.as_bytes().to_vec()));
                        }
                        if schema.has_column("action_to") {
                            self.action_to.push(Some(a.to.as_bytes().to_vec()));
                        }
                        if schema.has_column("action_value") {
                            self.action_value.push(a.value);
                        }
                        if schema.has_column("action_gas") {
                            self.action_gas.push(Some(a.gas.as_u32()));
                        }
                        if schema.has_column("action_input") {
                            self.action_input.push(Some(a.input.to_vec()));
                        }
                        if schema.has_column("action_call_type") {
                            self.action_call_type
                                .push(Some(traces::action_call_type_to_string(&a.call_type)));
                        }

                        if schema.has_column("action_init") {
                            self.action_init.push(None)
                        }
                        if schema.has_column("action_reward_type") {
                            self.action_reward_type.push(None)
                        }
                    }
                    Action::Create(action) => {
                        if schema.has_column("action_from") {
                            self.action_from.push(Some(action.from.as_bytes().to_vec()));
                        }
                        if schema.has_column("action_value") {
                            self.action_value.push(action.value);
                        }
                        if schema.has_column("action_gas") {
                            self.action_gas.push(Some(action.gas.as_u32()));
                        }
                        if schema.has_column("action_init") {
                            self.action_init.push(Some(action.init.to_vec()));
                        }

                        if schema.has_column("action_to") {
                            self.action_to.push(None)
                        }
                        if schema.has_column("action_input") {
                            self.action_input.push(None)
                        }
                        if schema.has_column("action_call_type") {
                            self.action_call_type.push(None)
                        }
                        if schema.has_column("action_reward_type") {
                            self.action_reward_type.push(None)
                        }
                    }
                    Action::Suicide(action) => {
                        if schema.has_column("action_from") {
                            self.action_from.push(Some(action.address.as_bytes().to_vec()));
                        }
                        if schema.has_column("action_to") {
                            self.action_to.push(Some(action.refund_address.as_bytes().to_vec()));
                        }
                        if schema.has_column("action_value") {
                            self.action_value.push(action.balance);
                        }

                        if schema.has_column("action_gas") {
                            self.action_gas.push(None)
                        }
                        if schema.has_column("action_input") {
                            self.action_input.push(None)
                        }
                        if schema.has_column("action_call_type") {
                            self.action_call_type.push(None)
                        }
                        if schema.has_column("action_init") {
                            self.action_init.push(None)
                        }
                        if schema.has_column("action_reward_type") {
                            self.action_reward_type.push(None)
                        }
                    }
                    Action::Reward(action) => {
                        if schema.has_column("action_to") {
                            self.action_to.push(Some(action.author.as_bytes().to_vec()));
                        }
                        if schema.has_column("action_value") {
                            self.action_value.push(action.value);
                        }
                        if schema.has_column("action_reward_type") {
                            self.action_reward_type
                                .push(Some(traces::reward_type_to_string(&action.reward_type)));
                        }

                        if schema.has_column("action_from") {
                            self.action_from.push(None)
                        }
                        if schema.has_column("action_gas") {
                            self.action_gas.push(None)
                        }
                        if schema.has_column("action_input") {
                            self.action_input.push(None)
                        }
                        if schema.has_column("action_call_type") {
                            self.action_call_type.push(None)
                        }
                        if schema.has_column("action_init") {
                            self.action_init.push(None)
                        }
                    }
                }
                if schema.has_column("action_type") {
                    self.action_type.push(traces::action_type_to_string(&tx_trace.action_type));
                }

                match &tx_trace.result {
                    Some(Res::Call(result)) => {
                        if schema.has_column("result_gas_used") {
                            self.result_gas_used.push(Some(result.gas_used.as_u32()));
                        }
                        if schema.has_column("result_output") {
                            self.result_output.push(Some(result.output.to_vec()));
                        }

                        if schema.has_column("result_code") {
                            self.result_code.push(None);
                        }
                        if schema.has_column("result_address") {
                            self.result_address.push(None);
                        }
                    }
                    Some(Res::Create(result)) => {
                        if schema.has_column("result_gas_used") {
                            self.result_gas_used.push(Some(result.gas_used.as_u32()));
                        }
                        if schema.has_column("result_code") {
                            self.result_code.push(Some(result.code.to_vec()));
                        }
                        if schema.has_column("result_address") {
                            self.result_address.push(Some(result.address.as_bytes().to_vec()));
                        }

                        if schema.has_column("result_output") {
                            self.result_output.push(None);
                        }
                    }
                    Some(Res::None) | None => {
                        if schema.has_column("result_gas_used") {
                            self.result_gas_used.push(None);
                        }
                        if schema.has_column("result_output") {
                            self.result_output.push(None);
                        }
                        if schema.has_column("result_code") {
                            self.result_code.push(None);
                        }
                        if schema.has_column("result_address") {
                            self.result_address.push(None);
                        }
                    }
                }
                if schema.has_column("trace_address") {
                    self.trace_address.push(
                        tx_trace
                            .trace_address
                            .iter()
                            .map(|n| n.to_string())
                            .collect::<Vec<String>>()
                            .join("_"),
                    );
                }
                if schema.has_column("subtraces") {
                    self.subtraces.push(tx_trace.subtraces as u32);
                }
                if schema.has_column("transaction_position") {
                    self.transaction_position.push(0_u32);
                }
                if schema.has_column("transaction_hash") {
                    self.transaction_hash
                        .push(output_data.transaction_hash.map(|x| x.as_bytes().to_vec()));
                }
                if schema.has_column("block_number") {
                    self.block_number.push(block_number as u32);
                }
                if schema.has_column("block_hash") {
                    self.block_hash.push(None);
                }
                if schema.has_column("error") {
                    self.error.push(tx_trace.error.clone());
                }
            }
        }
    }

    fn create_df(self, schema: &Table, chain_id: u64) -> Result<DataFrame, CollectError> {
        let mut cols = Vec::with_capacity(schema.columns().len());

        with_series_binary!(cols, "action_from", self.action_from, schema);
        with_series_binary!(cols, "action_to", self.action_to, schema);
        with_series_u256!(cols, "action_value", self.action_value, schema);
        with_series!(cols, "action_gas", self.action_gas, schema);
        with_series_binary!(cols, "action_input", self.action_input, schema);
        with_series!(cols, "action_call_type", self.action_call_type, schema);
        with_series_binary!(cols, "action_init", self.action_init, schema);
        with_series!(cols, "action_reward_type", self.action_reward_type, schema);
        with_series!(cols, "action_type", self.action_type, schema);
        with_series!(cols, "result_gas_used", self.result_gas_used, schema);
        with_series_binary!(cols, "result_output", self.result_output, schema);
        with_series_binary!(cols, "result_code", self.result_code, schema);
        with_series_binary!(cols, "result_address", self.result_address, schema);
        with_series!(cols, "trace_address", self.trace_address, schema);
        with_series!(cols, "subtraces", self.subtraces, schema);
        with_series!(cols, "transaction_position", self.transaction_position, schema);
        with_series_binary!(cols, "transaction_hash", self.transaction_hash, schema);
        with_series!(cols, "block_number", self.block_number, schema);
        with_series_binary!(cols, "block_hash", self.block_hash, schema);
        with_series!(cols, "error", self.error, schema);
        with_series_binary!(cols, "tx_to_address", self.tx_to_address, schema);
        with_series_binary!(cols, "tx_call_data", self.tx_call_data, schema);
        with_series!(cols, "chain_id", vec![chain_id; self.n_rows], schema);

        DataFrame::new(cols).map_err(CollectError::PolarsError).sort_by_schema(schema)
    }
}
