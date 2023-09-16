// reqired args:: address, contract

// single erc20 / many erc20s
// single block / many blocks
// single address / many addresses

use crate::{types::Erc20Balances, ColumnType, Dataset, Datatype};
use std::collections::HashMap;

use ethers::prelude::*;
use polars::prelude::*;
use tokio::sync::mpsc;

use crate::{
    dataframes::SortableDataFrame,
    types::{
        conversions::{ToVecHex, ToVecU8},
        AddressChunk, BlockChunk, CallDataChunk, CollectError, RowFilter, Source, Table,
    },
    with_series, with_series_binary, with_series_u256, ColumnEncoding,
};

use super::eth_calls;
use crate::U256Type;

#[async_trait::async_trait]
impl Dataset for Erc20Balances {
    fn datatype(&self) -> Datatype {
        Datatype::Erc20Balances
    }

    fn name(&self) -> &'static str {
        "erc20_balances"
    }

    fn column_types(&self) -> HashMap<&'static str, ColumnType> {
        HashMap::from_iter(vec![
            ("block_number", ColumnType::UInt32),
            ("erc20", ColumnType::Binary),
            ("address", ColumnType::Binary),
            ("balance", ColumnType::UInt256),
            ("chain_id", ColumnType::UInt64),
        ])
    }

    fn default_columns(&self) -> Vec<&'static str> {
        vec!["block_number", "erc20", "address", "balance", "chain_id"]
    }

    fn default_sort(&self) -> Vec<String> {
        vec!["block_number".to_string()]
    }

    async fn collect_block_chunk(
        &self,
        chunk: &BlockChunk,
        source: &Source,
        schema: &Table,
        filter: Option<&RowFilter>,
    ) -> Result<DataFrame, CollectError> {
        let (contract_chunks, call_data_chunks) = match filter {
            Some(filter) => {
                (filter.contract_chunks()?, create_balance_of_call_datas(filter.address_chunks()?)?)
            }
            None => return Err(CollectError::CollectError("must specify RowFilter".to_string())),
        };

        let rx = eth_calls::fetch_eth_calls(vec![chunk], contract_chunks, call_data_chunks, source)
            .await;
        balance_calls_to_df(rx, schema, source.chain_id).await
    }
}

fn create_balance_of_call_datas(
    address_chunks: Vec<AddressChunk>,
) -> Result<Vec<CallDataChunk>, CollectError> {
    let signature: Vec<u8> = prefix_hex::decode("0x70a08231").expect("Decoding failed");
    let mut call_data_chunks: Vec<CallDataChunk> = Vec::new();
    for address_chunk in address_chunks.iter() {
        match address_chunk {
            AddressChunk::Values(addresses) => {
                let call_datas: Vec<Vec<u8>> = addresses
                    .iter()
                    .map(|a| {
                        let mut call_data = signature.clone();
                        call_data.extend(a);
                        call_data
                    })
                    .collect();
                call_data_chunks.push(CallDataChunk::Values(call_datas))
            }
            _ => return Err(CollectError::CollectError("bad AddressChunk".to_string())),
        }
    }
    Ok(call_data_chunks)
}

async fn balance_calls_to_df(
    mut stream: mpsc::Receiver<Result<eth_calls::CallDataOutput, CollectError>>,
    schema: &Table,
    chain_id: u64,
) -> Result<DataFrame, CollectError> {
    // initialize
    let mut columns = Erc20BalanceColumns::default();

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
struct Erc20BalanceColumns {
    n_rows: usize,
    block_number: Vec<u32>,
    erc20: Vec<Vec<u8>>,
    address: Vec<Vec<u8>>,
    balance: Vec<U256>,
}

impl Erc20BalanceColumns {
    fn process_calls(&mut self, call_data_output: eth_calls::CallDataOutput, schema: &Table) {
        let (block_number, contract_address, call_data, output_data) = call_data_output;
        self.n_rows += 1;
        if schema.has_column("block_number") {
            self.block_number.push(block_number as u32);
        }
        if schema.has_column("erc20") {
            self.erc20.push(contract_address);
        }
        if schema.has_column("address") {
            self.address.push(call_data);
        }
        if schema.has_column("balance") {
            self.balance.push(output_data.to_vec().as_slice().into());
        }
    }

    fn create_df(self, schema: &Table, chain_id: u64) -> Result<DataFrame, CollectError> {
        let mut cols = Vec::with_capacity(schema.columns().len());
        with_series!(cols, "block_number", self.block_number, schema);
        with_series_binary!(cols, "erc20", self.erc20, schema);
        with_series_binary!(cols, "address", self.address, schema);
        with_series_u256!(cols, "balance", self.balance, schema);
        with_series!(cols, "chain_id", vec![chain_id; self.n_rows], schema);

        DataFrame::new(cols).map_err(CollectError::PolarsError).sort_by_schema(schema)
    }
}
