use super::traces;
use crate::*;
use alloy::{
    primitives::{keccak256, Address},
    rpc::types::trace::parity::{Action, LocalizedTransactionTrace, TraceOutput},
};
use polars::prelude::*;

/// columns for transactions
#[derive(Default, cryo_to_df::ToDataFrames)]
pub struct Contracts {
    n_rows: u64,
    block_number: Vec<u32>,
    block_hash: Vec<RawBytes>,
    create_index: Vec<u32>,
    transaction_hash: Vec<Option<RawBytes>>,
    contract_address: Vec<RawBytes>,
    deployer: Vec<RawBytes>,
    factory: Vec<RawBytes>,
    init_code: Vec<RawBytes>,
    code: Vec<RawBytes>,
    init_code_hash: Vec<RawBytes>,
    n_init_code_bytes: Vec<u32>,
    n_code_bytes: Vec<u32>,
    code_hash: Vec<RawBytes>,
    chain_id: Vec<u64>,
}

#[async_trait::async_trait]
impl Dataset for Contracts {
    fn default_sort() -> Option<Vec<&'static str>> {
        Some(vec!["block_number", "create_index"])
    }
}

#[async_trait::async_trait]
impl CollectByBlock for Contracts {
    type Response = Vec<LocalizedTransactionTrace>;

    async fn extract(request: Params, source: Arc<Source>, _: Arc<Query>) -> R<Self::Response> {
        source.trace_block(request.ethers_block_number()?).await
    }

    fn transform(response: Self::Response, columns: &mut Self, query: &Arc<Query>) -> R<()> {
        let traces =
            if query.exclude_failed { traces::filter_failed_traces(response) } else { response };
        process_contracts(&traces, columns, &query.schemas)
    }
}

#[async_trait::async_trait]
impl CollectByTransaction for Contracts {
    type Response = Vec<LocalizedTransactionTrace>;

    async fn extract(request: Params, source: Arc<Source>, _: Arc<Query>) -> R<Self::Response> {
        source.trace_transaction(request.ethers_transaction_hash()?).await
    }

    fn transform(response: Self::Response, columns: &mut Self, query: &Arc<Query>) -> R<()> {
        let traces =
            if query.exclude_failed { traces::filter_failed_traces(response) } else { response };
        process_contracts(&traces, columns, &query.schemas)
    }
}

/// process block into columns
pub(crate) fn process_contracts(
    traces: &[LocalizedTransactionTrace],
    columns: &mut Contracts,
    schemas: &Schemas,
) -> R<()> {
    let schema = schemas.get(&Datatype::Contracts).ok_or(err("schema not provided"))?;
    let mut deployer = Address::ZERO;
    let mut create_index = 0;
    for trace in traces.iter() {
        if trace.trace.trace_address.is_empty() {
            deployer = match &trace.trace.action {
                Action::Call(call) => call.from,
                Action::Create(create) => create.from,
                Action::Selfdestruct(suicide) => suicide.refund_address,
                Action::Reward(reward) => reward.author,
            };
        };

        if let (Action::Create(create), Some(TraceOutput::Create(result))) =
            (&trace.trace.action, &trace.trace.result)
        {
            columns.n_rows += 1;
            store!(schema, columns, block_number, trace.block_number.unwrap() as u32);
            store!(schema, columns, block_hash, trace.block_hash.unwrap().to_vec());
            store!(schema, columns, create_index, create_index);
            create_index += 1;
            let tx = trace.transaction_hash;
            store!(schema, columns, transaction_hash, tx.map(|x| x.to_vec()));
            store!(schema, columns, contract_address, result.address.to_vec());
            store!(schema, columns, deployer, deployer.to_vec());
            store!(schema, columns, factory, create.from.to_vec());
            store!(schema, columns, init_code, create.init.to_vec());
            store!(schema, columns, code, result.code.to_vec());
            store!(schema, columns, init_code_hash, keccak256(result.code.clone()).to_vec());
            store!(schema, columns, code_hash, keccak256(create.init.clone()).to_vec());
            store!(schema, columns, n_init_code_bytes, create.init.len() as u32);
            store!(schema, columns, n_code_bytes, result.code.len() as u32);
        }
    }
    Ok(())
}
