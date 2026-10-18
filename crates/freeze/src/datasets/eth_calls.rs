use crate::*;
use alloy::{
    primitives::{keccak256, TxKind},
    rpc::types::{TransactionInput, TransactionRequest},
};
use polars::prelude::*;

/// columns for transactions
#[derive(Default, cryo_to_df::ToDataFrames)]
pub struct EthCalls {
    n_rows: u64,
    block_number: Vec<u32>,
    contract_address: Vec<RawBytes>,
    call_data: Vec<RawBytes>,
    call_data_hash: Vec<RawBytes>,
    output_data: Vec<Option<RawBytes>>,
    output_data_hash: Vec<Option<RawBytes>>,
    chain_id: Vec<u64>,
}

#[async_trait::async_trait]
impl Dataset for EthCalls {
    fn default_columns() -> Option<Vec<&'static str>> {
        Some(vec!["block_number", "contract_address", "call_data", "output_data", "chain_id"])
    }

    fn default_sort() -> Option<Vec<&'static str>> {
        Some(vec!["block_number", "contract_address"])
    }

    fn default_blocks() -> Option<String> {
        Some("latest".to_string())
    }

    fn arg_aliases() -> Option<std::collections::HashMap<Dim, Dim>> {
        Some([(Dim::Address, Dim::Contract), (Dim::ToAddress, Dim::Contract)].into_iter().collect())
    }

    fn required_parameters() -> Vec<Dim> {
        vec![Dim::Contract, Dim::CallData]
    }
}

type EthCallsResponse = (u32, RawBytes, RawBytes, Option<RawBytes>);

#[async_trait::async_trait]
impl CollectByBlock for EthCalls {
    type Response = EthCallsResponse;

    async fn extract(request: Params, source: Arc<Source>, _: Arc<Query>) -> R<Self::Response> {
        let transaction = TransactionRequest {
            to: Some(TxKind::Call(request.ethers_contract()?)),
            input: TransactionInput::new(request.call_data()?.into()),
            ..Default::default()
        };
        let number = request.block_number()?;
        let output = source.call(transaction, number).await.ok().map(|x| x.to_vec());
        Ok((number as u32, request.contract()?, request.call_data()?, output))
    }

    fn transform(response: Self::Response, columns: &mut Self, query: &Arc<Query>) -> R<()> {
        let schema = query.schemas.get_schema(&Datatype::EthCalls)?;
        process_eth_call(response, columns, schema);
        Ok(())
    }
}

impl CollectByTransaction for EthCalls {
    type Response = ();
}

fn process_eth_call(response: EthCallsResponse, columns: &mut EthCalls, schema: &Table) {
    let (block_number, contract_address, call_data, output_data) = response;
    columns.n_rows += 1;
    store!(schema, columns, block_number, block_number);
    store!(schema, columns, contract_address, contract_address);
    store!(schema, columns, call_data, call_data.clone());
    store!(schema, columns, call_data_hash, keccak256(call_data).to_vec());
    store!(schema, columns, output_data, output_data.clone());
    store!(schema, columns, output_data_hash, output_data.map(|data| keccak256(data).to_vec()));
}
