use crate::*;
use ethers::prelude::*;
use polars::prelude::*;
use std::collections::HashMap;

/// columns for transactions
#[cryo_to_df::to_df(Datatype::EthCalls)]
#[derive(Default)]
pub struct EthCalls {
    n_rows: u64,
    block_number: Vec<u32>,
    contract_address: Vec<Vec<u8>>,
    call_data: Vec<Vec<u8>>,
    call_data_hash: Vec<Vec<u8>>,
    output_data: Vec<Vec<u8>>,
    output_data_hash: Vec<Vec<u8>>,
    chain_id: Vec<u64>,
}

#[async_trait::async_trait]
impl Dataset for EthCalls {
    fn name() -> &'static str {
        "eth_calls"
    }

    fn default_columns() -> Option<Vec<&'static str>> {
        Some(vec!["block_number", "contract_address", "call_data", "output_data", "chain_id"])
    }

    fn default_sort() -> Vec<String> {
        vec!["block_number".to_string(), "contract_address".to_string()]
    }

    fn default_blocks() -> Option<String> {
        Some("latest".to_string())
    }

    fn arg_aliases() -> Option<HashMap<Dim, Dim>> {
        Some([(Dim::Address, Dim::Contract), (Dim::ToAddress, Dim::Contract)].into_iter().collect())
    }

    fn required_parameters() -> Vec<Dim> {
        vec![Dim::Contract, Dim::CallData]
    }
}

type Result<T> = ::core::result::Result<T, CollectError>;

type EthCallsResponse = (u32, Vec<u8>, Vec<u8>, Vec<u8>);

#[async_trait::async_trait]
impl CollectByBlock for EthCalls {
    type Response = EthCallsResponse;

    async fn extract(
        request: Params,
        source: Arc<Source>,
        _schemas: Schemas,
    ) -> Result<Self::Response> {
        let transaction = TransactionRequest {
            to: Some(request.ethers_contract()?.into()),
            data: Some(request.call_data()?.into()),
            ..Default::default()
        };
        let number = request.block_number()?;
        let output = source.fetcher.call(transaction, number.into()).await?;
        Ok((number as u32, request.contract()?, request.call_data()?, output.to_vec()))
    }

    fn transform(response: Self::Response, columns: &mut Self, schemas: &Schemas) -> Result<()> {
        let schema = schemas.get(&Datatype::EthCalls).ok_or(err("schema not provided"))?;
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
    store!(schema, columns, call_data_hash, ethers_core::utils::keccak256(call_data).into());
    store!(schema, columns, output_data, output_data.to_vec());
    store!(schema, columns, output_data_hash, ethers_core::utils::keccak256(output_data).into());
}
