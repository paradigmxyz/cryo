use crate::*;
use ethers::prelude::*;
use polars::prelude::*;
use std::collections::HashMap;

/// columns for transactions
#[cryo_to_df::to_df(Datatype::Traces)]
#[derive(Default)]
pub struct Traces {
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
    transaction_index: Vec<Option<u32>>,
    transaction_hash: Vec<Option<Vec<u8>>>,
    block_number: Vec<u32>,
    block_hash: Vec<Vec<u8>>,
    error: Vec<Option<String>>,
    chain_id: Vec<u64>,
}

#[async_trait::async_trait]
impl Dataset for Traces {
    fn name() -> &'static str {
        "traces"
    }

    fn default_sort() -> Vec<String> {
        vec!["block_number".to_string(), "transaction_index".to_string()]
    }
}

type Result<T> = ::core::result::Result<T, CollectError>;

#[async_trait::async_trait]
impl CollectByBlock for Traces {
    type Response = Vec<Trace>;

    async fn extract(
        request: Params,
        source: Arc<Source>,
        _schemas: Schemas,
    ) -> Result<Self::Response> {
        source.fetcher.trace_block(request.block_number()?.into()).await
    }

    fn transform(response: Self::Response, columns: &mut Self, schemas: &Schemas) -> Result<()> {
        let traces = traces::filter_failed_traces(response);
        process_traces(&traces, columns, schemas)
    }
}

#[async_trait::async_trait]
impl CollectByTransaction for Traces {
    type Response = Vec<Trace>;

    async fn extract(
        request: Params,
        source: Arc<Source>,
        _schemas: Schemas,
    ) -> Result<Self::Response> {
        source.fetcher.trace_transaction(request.ethers_transaction_hash()?).await
    }

    fn transform(response: Self::Response, columns: &mut Self, schemas: &Schemas) -> Result<()> {
        let traces = traces::filter_failed_traces(response);
        process_traces(&traces, columns, schemas)
    }
}
/// process block into columns
pub(crate) fn process_traces(
    traces: &[Trace],
    columns: &mut Traces,
    schemas: &Schemas,
) -> Result<()> {
    let schema = schemas.get(&Datatype::Traces).ok_or(err("schema not provided"))?;
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
        store!(schema, columns, transaction_index, trace.transaction_position.map(|x| x as u32));
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
    Ok(())
}

fn process_action(action: &Action, columns: &mut Traces, schema: &Table) {
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

fn process_result(result: &Option<Res>, columns: &mut Traces, schema: &Table) {
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
        RewardType::EmptyStep => "empty_step".to_string(),
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

/// filter out error traces
pub(crate) fn filter_failed_traces(traces: Vec<Trace>) -> Vec<Trace> {
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
