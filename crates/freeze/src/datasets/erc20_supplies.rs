use crate::{
    conversions::{ToVecHex, ToVecU8},
    types::Erc20Supplies,
    ColumnType, Dataset, Datatype,
};
use std::collections::HashMap;
use tokio::sync::mpsc;

use ethers::prelude::*;
use polars::prelude::*;

use crate::{
    dataframes::SortableDataFrame,
    types::{BlockChunk, CollectError, RowFilter, Source, Table},
    with_series, with_series_binary, with_series_u256, CallDataChunk, ColumnEncoding, U256Type,
};

use super::eth_calls;

#[async_trait::async_trait]
impl Dataset for Erc20Supplies {
    fn datatype(&self) -> Datatype {
        Datatype::Erc20Supplies
    }

    fn name(&self) -> &'static str {
        "erc20_supplies"
    }

    fn column_types(&self) -> HashMap<&'static str, ColumnType> {
        HashMap::from_iter(vec![
            ("block_number", ColumnType::UInt32),
            ("erc20", ColumnType::Binary),
            ("total_supply", ColumnType::UInt256),
            ("chain_id", ColumnType::UInt64),
        ])
    }

    fn default_columns(&self) -> Vec<&'static str> {
        vec!["block_number", "erc20", "total_supply"]
    }

    fn default_sort(&self) -> Vec<String> {
        vec!["erc20".to_string(), "block_number".to_string()]
    }

    async fn collect_block_chunk(
        &self,
        chunk: &BlockChunk,
        source: &Source,
        schema: &Table,
        filter: Option<&RowFilter>,
    ) -> Result<DataFrame, CollectError> {
        let contract_chunks = match filter {
            Some(filter) => filter.contract_chunks()?,
            _ => return Err(CollectError::CollectError("must specify RowFilter".to_string())),
        };

        // build call data
        let call_data = prefix_hex::decode("0x18160ddd").expect("Decoding failed");
        let call_data_chunks = vec![CallDataChunk::Values(vec![call_data])];

        let rx = eth_calls::fetch_eth_calls(vec![chunk], contract_chunks, call_data_chunks, source)
            .await;
        supply_calls_to_df(rx, schema, source.chain_id).await
    }
}

async fn supply_calls_to_df(
    mut stream: mpsc::Receiver<Result<eth_calls::CallDataOutput, CollectError>>,
    schema: &Table,
    chain_id: u64,
) -> Result<DataFrame, CollectError> {
    // initialize
    let mut columns = SupplyColumns::default();

    // parse stream of blocks
    while let Some(message) = stream.recv().await {
        match message {
            Ok(call_data_output) => {
                columns.process_calls(call_data_output, schema);
            }
            Err(e) => {
                println!("{:?}", e);
                return Err(CollectError::TooManyRequestsError)
            }
        }
    }

    // convert to dataframes
    columns.create_df(schema, chain_id)
}

#[derive(Default)]
struct SupplyColumns {
    n_rows: usize,
    block_number: Vec<u32>,
    erc20: Vec<Vec<u8>>,
    total_supply: Vec<U256>,
}

impl SupplyColumns {
    fn process_calls(&mut self, call_data_output: eth_calls::CallDataOutput, schema: &Table) {
        let (block_number, contract_address, _call_data, output_data) = call_data_output;
        self.n_rows += 1;
        if schema.has_column("block_number") {
            self.block_number.push(block_number as u32);
        }
        if schema.has_column("erc20") {
            self.erc20.push(contract_address);
        }
        if schema.has_column("total_supply") {
            self.total_supply.push(output_data.to_vec().as_slice().into());
        }
    }

    fn create_df(self, schema: &Table, chain_id: u64) -> Result<DataFrame, CollectError> {
        let mut cols = Vec::with_capacity(schema.columns().len());
        with_series!(cols, "block_number", self.block_number, schema);
        with_series_binary!(cols, "erc20", self.erc20, schema);
        with_series_u256!(cols, "total_supply", self.total_supply, schema);
        with_series!(cols, "chain_id", vec![chain_id; self.n_rows], schema);

        DataFrame::new(cols).map_err(CollectError::PolarsError).sort_by_schema(schema)
    }
}
