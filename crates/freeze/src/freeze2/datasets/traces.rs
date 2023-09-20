use crate::{
    conversions::ToVecHex,
    dataframes::SortableDataFrame,
    freeze2::{ChunkDim, CollectByBlock, CollectByTransaction, ColumnData, RpcParams},
    store, with_series, with_series_binary, CollectError, ColumnType, Source, Table, Traces,
};
use ethers::prelude::*;
use polars::prelude::*;

#[async_trait::async_trait]
impl CollectByBlock for Traces {
    type BlockResponse = Vec<Trace>;

    type BlockColumns = TraceColumns;

    fn block_parameters() -> Vec<ChunkDim> {
        vec![ChunkDim::BlockNumber]
    }

    async fn extract_by_block(
        request: RpcParams,
        source: Source,
        _schema: Table,
    ) -> Result<Self::BlockResponse, CollectError> {
        source.fetcher.trace_block(request.block_number().into()).await
    }

    fn transform_by_block(
        response: Self::BlockResponse,
        columns: &mut Self::BlockColumns,
        schema: &Table,
    ) {
        process_traces(response, columns, schema)
    }
}

#[async_trait::async_trait]
impl CollectByTransaction for Traces {
    type TransactionResponse = Vec<Trace>;

    type TransactionColumns = TraceColumns;

    fn transaction_parameters() -> Vec<ChunkDim> {
        vec![ChunkDim::Transaction]
    }

    async fn extract_by_transaction(
        request: RpcParams,
        source: Source,
        _schema: Table,
    ) -> Result<Self::TransactionResponse, CollectError> {
        let tx_hash = H256::from_slice(&request.transaction());
        source.fetcher.trace_transaction(tx_hash).await
    }

    fn transform_by_transaction(
        response: Self::TransactionResponse,
        columns: &mut Self::TransactionColumns,
        schema: &Table,
    ) {
        process_traces(response, columns, schema)
    }
}

/// columns for transactions
#[cryo_to_df::to_df]
#[derive(Default)]
pub struct TraceColumns {
    n_rows: u64,
    action_from: Vec<Option<Vec<u8>>>,
    action_to: Vec<Option<Vec<u8>>>,
    action_value: Vec<String>,
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
    transaction_position: Vec<Option<u32>>,
    transaction_hash: Vec<Option<Vec<u8>>>,
    block_number: Vec<u32>,
    block_hash: Vec<Vec<u8>>,
    error: Vec<Option<String>>,
}

/// process block into columns
pub fn process_traces(traces: Vec<Trace>, columns: &mut TraceColumns, schema: &Table) {
    for trace in traces.iter() {
        columns.n_rows += 1;
        process_action(&trace.action, columns, schema);
        process_result(&trace.result, columns, schema);
        store!(schema, columns, action_type, action_type_to_string(&trace.action_type));
        store!(
            schema,
            columns,
            trace_address,
            trace.trace_address.iter().map(|n| n.to_string()).collect::<Vec<String>>().join("_")
        );
        store!(schema, columns, subtraces, trace.subtraces as u32);
        store!(schema, columns, transaction_position, trace.transaction_position.map(|x| x as u32));
        store!(
            schema,
            columns,
            transaction_hash,
            trace.transaction_hash.map(|x| x.as_bytes().to_vec())
        );
        store!(schema, columns, block_number, trace.block_number as u32);
        store!(schema, columns, block_hash, trace.block_hash.as_bytes().to_vec());
        store!(schema, columns, error, trace.error.clone());
    }
}

fn process_action(action: &Action, columns: &mut TraceColumns, schema: &Table) {
    match action {
        Action::Call(action) => {
            store!(schema, columns, action_from, Some(action.from.as_bytes().to_vec()));
            store!(schema, columns, action_to, Some(action.to.as_bytes().to_vec()));
            store!(schema, columns, action_value, action.value.to_string());
            store!(schema, columns, action_gas, Some(action.gas.as_u32()));
            store!(schema, columns, action_input, Some(action.input.to_vec()));
            store!(
                schema,
                columns,
                action_call_type,
                Some(action_call_type_to_string(&action.call_type))
            );
            store!(schema, columns, action_init, None);
            store!(schema, columns, action_reward_type, None);
        }
        Action::Create(action) => {
            store!(schema, columns, action_from, Some(action.from.as_bytes().to_vec()));
            store!(schema, columns, action_to, None);
            store!(schema, columns, action_value, action.value.to_string());
            store!(schema, columns, action_gas, Some(action.gas.as_u32()));
            store!(schema, columns, action_input, None);
            store!(schema, columns, action_call_type, None);
            store!(schema, columns, action_init, Some(action.init.to_vec()));
            store!(schema, columns, action_reward_type, None);
        }
        Action::Suicide(action) => {
            store!(schema, columns, action_from, Some(action.address.as_bytes().to_vec()));
            store!(schema, columns, action_to, Some(action.refund_address.as_bytes().to_vec()));
            store!(schema, columns, action_value, action.balance.to_string());
            store!(schema, columns, action_gas, None);
            store!(schema, columns, action_input, None);
            store!(schema, columns, action_call_type, None);
            store!(schema, columns, action_init, None);
            store!(schema, columns, action_reward_type, None);
        }
        Action::Reward(action) => {
            store!(schema, columns, action_from, Some(action.author.as_bytes().to_vec()));
            store!(schema, columns, action_to, None);
            store!(schema, columns, action_value, action.value.to_string());
            store!(schema, columns, action_gas, None);
            store!(schema, columns, action_input, None);
            store!(schema, columns, action_call_type, None);
            store!(schema, columns, action_init, None);
            store!(
                schema,
                columns,
                action_reward_type,
                Some(reward_type_to_string(&action.reward_type))
            );
        }
    }
}

fn process_result(result: &Option<Res>, columns: &mut TraceColumns, schema: &Table) {
    match result {
        Some(Res::Call(result)) => {
            store!(schema, columns, result_gas_used, Some(result.gas_used.as_u32()));
            store!(schema, columns, result_output, Some(result.output.to_vec()));
            store!(schema, columns, result_code, None);
            store!(schema, columns, result_address, None);
        }
        Some(Res::Create(result)) => {
            store!(schema, columns, result_gas_used, Some(result.gas_used.as_u32()));
            store!(schema, columns, result_output, None);
            store!(schema, columns, result_code, Some(result.code.to_vec()));
            store!(schema, columns, result_address, Some(result.address.as_bytes().to_vec()));
        }
        Some(Res::None) | None => {
            store!(schema, columns, result_gas_used, None);
            store!(schema, columns, result_output, None);
            store!(schema, columns, result_code, None);
            store!(schema, columns, result_address, None);
        }
    }
}

pub(crate) fn reward_type_to_string(reward_type: &RewardType) -> String {
    match reward_type {
        RewardType::Block => "reward".to_string(),
        RewardType::Uncle => "uncle".to_string(),
        RewardType::EmptyStep => "emtpy_step".to_string(),
        RewardType::External => "external".to_string(),
    }
}

pub(crate) fn action_type_to_string(action_type: &ActionType) -> String {
    match action_type {
        ActionType::Call => "call".to_string(),
        ActionType::Create => "create".to_string(),
        ActionType::Reward => "reward".to_string(),
        ActionType::Suicide => "suicide".to_string(),
    }
}

pub(crate) fn action_call_type_to_string(action_call_type: &CallType) -> String {
    match action_call_type {
        CallType::None => "none".to_string(),
        CallType::Call => "call".to_string(),
        CallType::CallCode => "call_code".to_string(),
        CallType::DelegateCall => "delegate_call".to_string(),
        CallType::StaticCall => "static_call".to_string(),
    }
}
