use std::{collections::HashMap, sync::Arc};

use ethers::prelude::*;
use polars::prelude::*;
use tokio::{sync::mpsc, task};

use crate::{
    dataframes::SortableDataFrame,
    types::{
        conversions::ToVecHex, BlockChunk, CollectError, ColumnType, Dataset, Datatype, RowFilter,
        Source, Table, Traces,
    },
    with_series, with_series_binary,
};

#[async_trait::async_trait]
impl Dataset for Traces {
    fn datatype(&self) -> Datatype {
        Datatype::Traces
    }

    fn name(&self) -> &'static str {
        "traces"
    }

    fn column_types(&self) -> HashMap<&'static str, ColumnType> {
        HashMap::from_iter(vec![
            ("action_from", ColumnType::Binary),
            ("action_to", ColumnType::Binary),
            ("action_value", ColumnType::String),
            ("action_gas", ColumnType::UInt32),
            ("action_input", ColumnType::Binary),
            ("action_call_type", ColumnType::String),
            ("action_init", ColumnType::Binary),
            ("action_reward_type", ColumnType::String),
            ("action_type", ColumnType::String),
            ("result_gas_used", ColumnType::UInt32),
            ("result_output", ColumnType::Binary),
            ("result_code", ColumnType::Binary),
            ("result_address", ColumnType::Binary),
            ("trace_address", ColumnType::String),
            ("subtraces", ColumnType::UInt32),
            ("transaction_position", ColumnType::UInt32),
            ("transaction_hash", ColumnType::Binary),
            ("block_number", ColumnType::UInt32),
            ("block_hash", ColumnType::Binary),
            ("error", ColumnType::String),
            ("chain_id", ColumnType::UInt64),
        ])
    }

    fn default_columns(&self) -> Vec<&'static str> {
        vec![
            "action_from",
            "action_to",
            "action_value",
            "action_gas",
            "action_input",
            "action_call_type",
            "action_init",
            "action_reward_type",
            "action_type",
            "result_gas_used",
            "result_output",
            "result_code",
            "result_address",
            "trace_address",
            "subtraces",
            "transaction_position",
            "transaction_hash",
            "block_number",
            "block_hash",
            "error",
        ]
    }

    fn default_sort(&self) -> Vec<String> {
        vec!["block_number".to_string(), "transaction_position".to_string()]
    }

    async fn collect_block_chunk(
        &self,
        chunk: &BlockChunk,
        source: &Source,
        schema: &Table,
        _filter: Option<&RowFilter>,
    ) -> Result<DataFrame, CollectError> {
        let rx = fetch_traces(chunk, source).await;
        traces_to_df(rx, schema, source.chain_id).await
    }
}

async fn fetch_traces(
    block_chunk: &BlockChunk,
    source: &Source,
) -> mpsc::Receiver<Result<Vec<Trace>, CollectError>> {
    let (tx, rx) = mpsc::channel(block_chunk.numbers().len());

    for number in block_chunk.numbers() {
        let tx = tx.clone();
        let provider = source.provider.clone();
        let semaphore = source.semaphore.clone();
        let rate_limiter = source.rate_limiter.as_ref().map(Arc::clone);
        task::spawn(async move {
            let _permit = match semaphore {
                Some(semaphore) => Some(Arc::clone(&semaphore).acquire_owned().await),
                _ => None,
            };
            if let Some(limiter) = rate_limiter {
                Arc::clone(&limiter).until_ready().await;
            }
            let result = provider
                .trace_block(BlockNumber::Number(number.into()))
                .await
                .map_err(CollectError::ProviderError);
            match tx.send(result).await {
                Ok(_) => {}
                Err(tokio::sync::mpsc::error::SendError(_e)) => {
                    eprintln!("send error, try using a rate limit with --requests-per-second or limiting max concurrency with --max-concurrent-requests");
                    std::process::exit(1)
                }
            }
        });
    }
    rx
}

fn reward_type_to_string(reward_type: &RewardType) -> String {
    match reward_type {
        RewardType::Block => "reward".to_string(),
        RewardType::Uncle => "uncle".to_string(),
        RewardType::EmptyStep => "emtpy_step".to_string(),
        RewardType::External => "external".to_string(),
    }
}

fn action_type_to_string(action_type: &ActionType) -> String {
    match action_type {
        ActionType::Call => "call".to_string(),
        ActionType::Create => "create".to_string(),
        ActionType::Reward => "reward".to_string(),
        ActionType::Suicide => "suicide".to_string(),
    }
}

fn action_call_type_to_string(action_call_type: &CallType) -> String {
    match action_call_type {
        CallType::None => "none".to_string(),
        CallType::Call => "call".to_string(),
        CallType::CallCode => "call_code".to_string(),
        CallType::DelegateCall => "delegate_call".to_string(),
        CallType::StaticCall => "static_call".to_string(),
    }
}

async fn traces_to_df(
    mut rx: mpsc::Receiver<Result<Vec<Trace>, CollectError>>,
    schema: &Table,
    chain_id: u64,
) -> Result<DataFrame, CollectError> {
    let include_action_from = schema.has_column("action_from");
    let include_action_to = schema.has_column("action_to");
    let include_action_value = schema.has_column("action_value");
    let include_action_gas = schema.has_column("action_gas");
    let include_action_input = schema.has_column("action_input");
    let include_action_call_type = schema.has_column("action_call_type");
    let include_action_init = schema.has_column("action_init");
    let include_action_reward_type = schema.has_column("action_reward_type");
    let include_action_type = schema.has_column("action_type");
    let include_result_gas_used = schema.has_column("result_gas_used");
    let include_result_output = schema.has_column("result_output");
    let include_result_code = schema.has_column("result_code");
    let include_result_address = schema.has_column("result_address");
    let include_trace_address = schema.has_column("trace_address");
    let include_subtraces = schema.has_column("subtraces");
    let include_transaction_position = schema.has_column("transaction_position");
    let include_transaction_hash = schema.has_column("transaction_hash");
    let include_block_number = schema.has_column("block_number");
    let include_block_hash = schema.has_column("block_hash");
    let include_error = schema.has_column("error");

    let capacity = 0;
    let mut action_from: Vec<Option<Vec<u8>>> = Vec::with_capacity(capacity);
    let mut action_to: Vec<Option<Vec<u8>>> = Vec::with_capacity(capacity);
    let mut action_value: Vec<String> = Vec::with_capacity(capacity);
    let mut action_gas: Vec<Option<u32>> = Vec::with_capacity(capacity);
    let mut action_input: Vec<Option<Vec<u8>>> = Vec::with_capacity(capacity);
    let mut action_call_type: Vec<Option<String>> = Vec::with_capacity(capacity);
    let mut action_init: Vec<Option<Vec<u8>>> = Vec::with_capacity(capacity);
    let mut action_reward_type: Vec<Option<String>> = Vec::with_capacity(capacity);
    let mut action_type: Vec<String> = Vec::with_capacity(capacity);
    let mut result_gas_used: Vec<Option<u32>> = Vec::with_capacity(capacity);
    let mut result_output: Vec<Option<Vec<u8>>> = Vec::with_capacity(capacity);
    let mut result_code: Vec<Option<Vec<u8>>> = Vec::with_capacity(capacity);
    let mut result_address: Vec<Option<Vec<u8>>> = Vec::with_capacity(capacity);
    let mut trace_address: Vec<String> = Vec::with_capacity(capacity);
    let mut subtraces: Vec<u32> = Vec::with_capacity(capacity);
    let mut transaction_position: Vec<u32> = Vec::with_capacity(capacity);
    let mut transaction_hash: Vec<Vec<u8>> = Vec::with_capacity(capacity);
    let mut block_number: Vec<u32> = Vec::with_capacity(capacity);
    let mut block_hash: Vec<Vec<u8>> = Vec::with_capacity(capacity);
    let mut error: Vec<Option<String>> = Vec::with_capacity(capacity);

    let mut n_rows = 0;
    while let Some(message) = rx.recv().await {
        match message {
            Ok(traces) => {
                for trace in traces.iter() {
                    if let (Some(tx_hash), Some(tx_pos)) =
                        (trace.transaction_hash, trace.transaction_position)
                    {
                        n_rows += 1;

                        // Call
                        // from: from,
                        // to: to,
                        // value: value,
                        // gas: gas,
                        // input: input,
                        // call_type: action_call_type, [None, Call, CallCode, DelegateCall,
                        // StaticCall]
                        //
                        // Create
                        // from: from,
                        // value: value,
                        // gas: gas,
                        // init: init,
                        //
                        // Suicide
                        // address: from,
                        // refund_address: to,
                        // balance: value,
                        //
                        // Reward
                        // author: to,
                        // value: value,
                        // reward_type: action_reward_type, [Block, Uncle, EmptyStep, External],

                        match &trace.action {
                            Action::Call(a) => {
                                if include_action_from {
                                    action_from.push(Some(a.from.as_bytes().to_vec()));
                                }
                                if include_action_to {
                                    action_to.push(Some(a.to.as_bytes().to_vec()));
                                }
                                if include_action_value {
                                    action_value.push(a.value.to_string());
                                }
                                if include_action_gas {
                                    action_gas.push(Some(a.gas.as_u32()));
                                }
                                if include_action_input {
                                    action_input.push(Some(a.input.to_vec()));
                                }
                                if include_action_call_type {
                                    action_call_type
                                        .push(Some(action_call_type_to_string(&a.call_type)));
                                }

                                if include_action_init {
                                    action_init.push(None)
                                }
                                if include_action_reward_type {
                                    action_reward_type.push(None)
                                }
                            }
                            Action::Create(action) => {
                                if include_action_from {
                                    action_from.push(Some(action.from.as_bytes().to_vec()));
                                }
                                if include_action_value {
                                    action_value.push(action.value.to_string());
                                }
                                if include_action_gas {
                                    action_gas.push(Some(action.gas.as_u32()));
                                }
                                if include_action_init {
                                    action_init.push(Some(action.init.to_vec()));
                                }

                                if include_action_to {
                                    action_to.push(None)
                                }
                                if include_action_input {
                                    action_input.push(None)
                                }
                                if include_action_call_type {
                                    action_call_type.push(None)
                                }
                                if include_action_reward_type {
                                    action_reward_type.push(None)
                                }
                            }
                            Action::Suicide(action) => {
                                if include_action_from {
                                    action_from.push(Some(action.address.as_bytes().to_vec()));
                                }
                                if include_action_to {
                                    action_to.push(Some(action.refund_address.as_bytes().to_vec()));
                                }
                                if include_action_value {
                                    action_value.push(action.balance.to_string());
                                }

                                if include_action_gas {
                                    action_gas.push(None)
                                }
                                if include_action_input {
                                    action_input.push(None)
                                }
                                if include_action_call_type {
                                    action_call_type.push(None)
                                }
                                if include_action_init {
                                    action_init.push(None)
                                }
                                if include_action_reward_type {
                                    action_reward_type.push(None)
                                }
                            }
                            Action::Reward(action) => {
                                if include_action_to {
                                    action_to.push(Some(action.author.as_bytes().to_vec()));
                                }
                                if include_action_value {
                                    action_value.push(action.value.to_string());
                                }
                                if include_action_reward_type {
                                    action_reward_type
                                        .push(Some(reward_type_to_string(&action.reward_type)));
                                }

                                if include_action_from {
                                    action_from.push(None)
                                }
                                if include_action_gas {
                                    action_gas.push(None)
                                }
                                if include_action_input {
                                    action_input.push(None)
                                }
                                if include_action_call_type {
                                    action_call_type.push(None)
                                }
                                if include_action_init {
                                    action_init.push(None)
                                }
                            }
                        }
                        if include_action_type {
                            action_type.push(action_type_to_string(&trace.action_type));
                        }

                        match &trace.result {
                            Some(Res::Call(result)) => {
                                if include_result_gas_used {
                                    result_gas_used.push(Some(result.gas_used.as_u32()));
                                }
                                if include_result_output {
                                    result_output.push(Some(result.output.to_vec()));
                                }

                                if include_result_code {
                                    result_code.push(None);
                                }
                                if include_result_address {
                                    result_address.push(None);
                                }
                            }
                            Some(Res::Create(result)) => {
                                if include_result_gas_used {
                                    result_gas_used.push(Some(result.gas_used.as_u32()));
                                }
                                if include_result_code {
                                    result_code.push(Some(result.code.to_vec()));
                                }
                                if include_result_address {
                                    result_address.push(Some(result.address.as_bytes().to_vec()));
                                }

                                if include_result_output {
                                    result_output.push(None);
                                }
                            }
                            Some(Res::None) | None => {
                                if include_result_gas_used {
                                    result_gas_used.push(None);
                                }
                                if include_result_output {
                                    result_output.push(None);
                                }
                                if include_result_code {
                                    result_code.push(None);
                                }
                                if include_result_address {
                                    result_address.push(None);
                                }
                            }
                        }
                        if include_trace_address {
                            trace_address.push(
                                trace
                                    .trace_address
                                    .iter()
                                    .map(|n| n.to_string())
                                    .collect::<Vec<String>>()
                                    .join("_"),
                            );
                        }
                        if include_subtraces {
                            subtraces.push(trace.subtraces as u32);
                        }
                        if include_transaction_position {
                            transaction_position.push(tx_pos as u32);
                        }
                        if include_transaction_hash {
                            transaction_hash.push(tx_hash.as_bytes().to_vec());
                        }
                        if include_block_number {
                            block_number.push(trace.block_number as u32);
                        }
                        if include_block_hash {
                            block_hash.push(trace.block_hash.as_bytes().to_vec());
                        }
                        if include_error {
                            error.push(trace.error.clone());
                        }
                    }
                }
            }
            _ => return Err(CollectError::TooManyRequestsError),
        }
    }

    let mut cols = Vec::new();

    with_series_binary!(cols, "action_from", action_from, schema);
    with_series_binary!(cols, "action_to", action_to, schema);
    with_series!(cols, "action_value", action_value, schema);
    with_series!(cols, "action_gas", action_gas, schema);
    with_series_binary!(cols, "action_input", action_input, schema);
    with_series!(cols, "action_call_type", action_call_type, schema);
    with_series_binary!(cols, "action_init", action_init, schema);
    with_series!(cols, "action_reward_type", action_reward_type, schema);
    with_series!(cols, "action_type", action_type, schema);
    with_series!(cols, "result_gas_used", result_gas_used, schema);
    with_series_binary!(cols, "result_output", result_output, schema);
    with_series_binary!(cols, "result_code", result_code, schema);
    with_series_binary!(cols, "result_address", result_address, schema);
    with_series!(cols, "trace_address", trace_address, schema);
    with_series!(cols, "subtraces", subtraces, schema);
    with_series!(cols, "transaction_position", transaction_position, schema);
    with_series_binary!(cols, "transaction_hash", transaction_hash, schema);
    with_series!(cols, "block_number", block_number, schema);
    with_series_binary!(cols, "block_hash", block_hash, schema);
    with_series!(cols, "error", error, schema);

    if schema.has_column("chain_id") {
        cols.push(Series::new("chain_id", vec![chain_id; n_rows]));
    }

    DataFrame::new(cols).map_err(CollectError::PolarsError).sort_by_schema(schema)
}
