use super::traces;
use crate::*;
use alloy::rpc::types::trace::parity::{Action, TraceOutput, TraceType, TransactionTrace};
use polars::prelude::*;

/// columns for transactions
#[derive(Default, cryo_to_df::ToDataFrames)]
pub struct TraceCalls {
    n_rows: u64,
    block_number: Vec<u32>,
    transaction_index: Vec<u32>,
    action_from: Vec<Option<RawBytes>>,
    action_to: Vec<Option<RawBytes>>,
    action_value: Vec<String>,
    action_gas: Vec<Option<u32>>,
    action_input: Vec<Option<RawBytes>>,
    action_call_type: Vec<Option<String>>,
    action_init: Vec<Option<RawBytes>>,
    action_reward_type: Vec<Option<String>>,
    action_type: Vec<String>,
    result_gas_used: Vec<Option<u32>>,
    result_output: Vec<Option<RawBytes>>,
    result_code: Vec<Option<RawBytes>>,
    result_address: Vec<Option<RawBytes>>,
    trace_address: Vec<String>,
    subtraces: Vec<u32>,
    error: Vec<Option<String>>,
    tx_to_address: Vec<RawBytes>,
    tx_call_data: Vec<RawBytes>,
    chain_id: Vec<u64>,
}

impl Dataset for TraceCalls {
    fn default_blocks() -> Option<String> {
        Some("latest".to_string())
    }

    fn required_parameters() -> Vec<Dim> {
        vec![Dim::Contract, Dim::CallData]
    }

    fn arg_aliases() -> Option<std::collections::HashMap<Dim, Dim>> {
        Some([(Dim::Address, Dim::Contract), (Dim::ToAddress, Dim::Contract)].into_iter().collect())
    }
}

#[async_trait::async_trait]
impl CollectByBlock for TraceCalls {
    type Response = (u32, RawBytes, RawBytes, Vec<TransactionTrace>);

    async fn extract(request: Params, source: Arc<Source>, _: Arc<Query>) -> R<Self::Response> {
        let traces: Vec<TransactionTrace> = source
            .trace_call2(
                request.ethers_contract()?,
                request.call_data()?,
                vec![TraceType::Trace],
                Some(request.ethers_block_number()?),
            )
            .await?
            .trace;
        Ok((request.block_number()? as u32, request.contract()?, request.call_data()?, traces))
    }

    fn transform(response: Self::Response, columns: &mut Self, query: &Arc<Query>) -> R<()> {
        let schema = query.schemas.get_schema(&Datatype::TraceCalls)?;
        process_transaction_traces(response, columns, schema);
        Ok(())
    }
}

impl CollectByTransaction for TraceCalls {
    type Response = ();
}

fn process_transaction_traces(
    response: (u32, RawBytes, RawBytes, Vec<TransactionTrace>),
    columns: &mut TraceCalls,
    schema: &Table,
) {
    let (block_number, contract, call_data, traces) = response;
    for (transaction_index, trace) in traces.iter().enumerate() {
        columns.n_rows += 1;

        process_action(&trace.action, columns, schema);
        process_result(&trace.result, columns, schema);
        store!(schema, columns, action_type, traces::action_type_to_string(&trace.action.kind()));
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
            store!(schema, columns, action_from, Some(action.from.to_vec()));
            store!(schema, columns, action_to, Some(action.to.to_vec()));
            store!(schema, columns, action_value, action.value.to_string());
            store!(schema, columns, action_gas, Some(action.gas as u32));
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
            store!(schema, columns, action_from, Some(action.from.to_vec()));
            store!(schema, columns, action_to, None);
            store!(schema, columns, action_value, action.value.to_string());
            store!(schema, columns, action_gas, Some(action.gas as u32));
            store!(schema, columns, action_input, None);
            store!(schema, columns, action_call_type, None);
            store!(schema, columns, action_init, Some(action.init.to_vec()));
            store!(schema, columns, action_reward_type, None);
        }
        Action::Selfdestruct(action) => {
            store!(schema, columns, action_from, Some(action.address.to_vec()));
            store!(schema, columns, action_to, Some(action.refund_address.to_vec()));
            store!(schema, columns, action_value, action.balance.to_string());
            store!(schema, columns, action_gas, None);
            store!(schema, columns, action_input, None);
            store!(schema, columns, action_call_type, None);
            store!(schema, columns, action_init, None);
            store!(schema, columns, action_reward_type, None);
        }
        Action::Reward(action) => {
            store!(schema, columns, action_from, Some(action.author.to_vec()));
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

fn process_result(result: &Option<TraceOutput>, columns: &mut TraceCalls, schema: &Table) {
    match result {
        Some(TraceOutput::Call(result)) => {
            store!(schema, columns, result_gas_used, Some(result.gas_used as u32));
            store!(schema, columns, result_output, Some(result.output.to_vec()));
            store!(schema, columns, result_code, None);
            store!(schema, columns, result_address, None);
        }
        Some(TraceOutput::Create(result)) => {
            store!(schema, columns, result_gas_used, Some(result.gas_used as u32));
            store!(schema, columns, result_output, None);
            store!(schema, columns, result_code, Some(result.code.to_vec()));
            store!(schema, columns, result_address, Some(result.address.to_vec()));
        }
        None => {
            store!(schema, columns, result_gas_used, None);
            store!(schema, columns, result_output, None);
            store!(schema, columns, result_code, None);
            store!(schema, columns, result_address, None);
        }
    }
}
