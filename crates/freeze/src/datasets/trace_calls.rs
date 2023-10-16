use super::traces;
use crate::*;
use ethers::prelude::*;
use polars::prelude::*;
use std::collections::HashMap;

/// columns for transactions
#[cryo_to_df::to_df(Datatype::TraceCalls)]
#[derive(Default)]
pub struct TraceCalls {
    n_rows: u64,
    block_number: Vec<u32>,
    transaction_index: Vec<u32>,
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
    error: Vec<Option<String>>,
    tx_to_address: Vec<Vec<u8>>,
    tx_call_data: Vec<Vec<u8>>,
    chain_id: Vec<u64>,
}

impl Dataset for TraceCalls {
    fn name() -> &'static str {
        "trace_calls"
    }

    fn default_sort() -> Vec<String> {
        vec!["block_number".to_string(), "transaction_index".to_string()]
    }

    fn default_blocks() -> Option<String> {
        Some("latest".to_string())
    }

    fn required_parameters() -> Vec<Dim> {
        vec![Dim::Contract, Dim::CallData]
    }

    fn arg_aliases() -> Option<HashMap<Dim, Dim>> {
        Some([(Dim::Address, Dim::Contract), (Dim::ToAddress, Dim::Contract)].into_iter().collect())
    }
}

type Result<T> = ::core::result::Result<T, CollectError>;
type ContractCallDataTraces = (u32, Vec<u8>, Vec<u8>, Vec<TransactionTrace>);

#[async_trait::async_trait]
impl CollectByBlock for TraceCalls {
    type Response = ContractCallDataTraces;

    async fn extract(
        request: Params,
        source: Arc<Source>,
        _schemas: Schemas,
    ) -> Result<Self::Response> {
        let traces: Vec<TransactionTrace> = source
            .fetcher
            .trace_call2(
                request.ethers_contract()?,
                request.call_data()?,
                vec![TraceType::Trace],
                Some(request.ethers_block_number()?),
            )
            .await?
            .trace
            .ok_or(CollectError::CollectError("traces missing".to_string()))?;
        Ok((request.block_number()? as u32, request.contract()?, request.call_data()?, traces))
    }

    fn transform(response: Self::Response, columns: &mut Self, schemas: &Schemas) -> Result<()> {
        let schema = schemas.get(&Datatype::TraceCalls).ok_or(err("schema not provided"))?;
        process_transaction_traces(response, columns, schema);
        Ok(())
    }
}

impl CollectByTransaction for TraceCalls {
    type Response = ();
}

fn process_transaction_traces(
    response: ContractCallDataTraces,
    columns: &mut TraceCalls,
    schema: &Table,
) {
    let (block_number, contract, call_data, traces) = response;
    for (transaction_index, trace) in traces.iter().enumerate() {
        columns.n_rows += 1;

        process_action(&trace.action, columns, schema);
        process_result(&trace.result, columns, schema);
        store!(schema, columns, action_type, traces::action_type_to_string(&trace.action_type));
        store!(
            schema,
            columns,
            trace_address,
            trace.trace_address.iter().map(|n| n.to_string()).collect::<Vec<String>>().join("_")
        );
        store!(schema, columns, subtraces, trace.subtraces as u32);
        store!(schema, columns, transaction_index, transaction_index as u32);
        store!(schema, columns, block_number, block_number);
        store!(schema, columns, error, trace.error.clone());
        store!(schema, columns, tx_to_address, contract.clone());
        store!(schema, columns, tx_call_data, call_data.clone());
    }
}

fn process_action(action: &Action, columns: &mut TraceCalls, schema: &Table) {
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
                Some(traces::action_call_type_to_string(&action.call_type))
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
                Some(traces::reward_type_to_string(&action.reward_type))
            );
        }
    }
}

fn process_result(result: &Option<Res>, columns: &mut TraceCalls, schema: &Table) {
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
