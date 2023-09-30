use crate::{
    conversions::ToVecHex, dataframes::SortableDataFrame, store, with_series, with_series_binary,
    CollectByBlock, CollectError, ColumnData, ColumnType, Dataset, Datatype, EthCalls, Params,
    Schemas, Source, Table,
};
use ethers::prelude::*;
use polars::prelude::*;
use std::collections::HashMap;

/// columns for transactions
#[cryo_to_df::to_df(Datatype::Transactions)]
#[derive(Default)]
pub struct EthCallColumns {
    n_rows: u64,
    block_number: Vec<u32>,
    contract_address: Vec<Vec<u8>>,
    call_data: Vec<Vec<u8>>,
    call_data_hash: Vec<Vec<u8>>,
    output_data: Vec<Vec<u8>>,
    output_data_hash: Vec<Vec<u8>>,
}

#[async_trait::async_trait]
impl Dataset for EthCalls {
    fn datatype(&self) -> Datatype {
        Datatype::EthCalls
    }

    fn name(&self) -> &'static str {
        "eth_calls"
    }

    fn column_types(&self) -> HashMap<&'static str, ColumnType> {
        HashMap::from_iter(vec![
            ("block_number", ColumnType::UInt32),
            ("contract_address", ColumnType::Binary),
            ("call_data", ColumnType::Binary),
            ("call_data_hash", ColumnType::Binary),
            ("output_data", ColumnType::Binary),
            ("output_data_hash", ColumnType::Binary),
            ("chain_id", ColumnType::UInt64),
        ])
    }

    fn default_columns(&self) -> Vec<&'static str> {
        vec!["block_number", "contract_address", "call_data", "output_data"]
    }

    fn default_sort(&self) -> Vec<String> {
        vec!["block_number".to_string(), "contract_address".to_string()]
    }

    fn default_blocks(&self) -> Option<String> {
        Some("latest".to_string())
    }

    fn arg_aliases(&self) -> HashMap<String, String> {
        [("address", "to_address"), ("contract", "to_address")]
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    }
}

type Result<T> = ::core::result::Result<T, CollectError>;

type EthCallsResponse = (u32, Vec<u8>, Vec<u8>, Vec<u8>);

#[async_trait::async_trait]
impl CollectByBlock for EthCalls {
    type Response = EthCallsResponse;

    type Columns = EthCallColumns;

    async fn extract(request: Params, source: Source, _schemas: Schemas) -> Result<Self::Response> {
        let transaction = TransactionRequest {
            to: Some(request.ethers_address().into()),
            data: Some(request.call_data().into()),
            ..Default::default()
        };
        let number = request.block_number();
        let output = source.fetcher.call(transaction, number.into()).await?;
        Ok((number as u32, request.address(), request.call_data(), output.to_vec()))
    }

    fn transform(response: Self::Response, columns: &mut Self::Columns, schemas: &Schemas) {
        let schema = schemas.get(&Datatype::EthCalls).expect("missing schema");
        process_eth_call(response, columns, schema)
    }
}

fn process_eth_call(response: EthCallsResponse, columns: &mut EthCallColumns, schema: &Table) {
    let (block_number, contract_address, call_data, output_data) = response;
    columns.n_rows += 1;
    store!(schema, columns, block_number, block_number);
    store!(schema, columns, contract_address, contract_address);
    store!(schema, columns, call_data, call_data.clone());
    store!(schema, columns, call_data_hash, ethers_core::utils::keccak256(call_data).into());
    store!(schema, columns, output_data, output_data.to_vec());
    store!(schema, columns, output_data_hash, ethers_core::utils::keccak256(output_data).into());
}
