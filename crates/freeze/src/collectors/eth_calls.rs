use crate::{
    conversions::ToVecHex, dataframes::SortableDataFrame, store, with_series, with_series_binary,
    CollectByBlock, CollectError, ColumnData, ColumnType, Datatype, EthCalls, Params, Schemas,
    Source, Table,
};
use ethers::prelude::*;
use polars::prelude::*;
use std::collections::HashMap;

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
