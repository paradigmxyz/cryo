// fix fetcher
// implement --dedup
use crate::{conversions::ToVecHex, ColumnType, Dataset, Datatype};
use std::collections::HashMap;
use tokio::{sync::mpsc, task};

use ethers::prelude::*;
use polars::prelude::*;

use crate::{
    dataframes::SortableDataFrame,
    types::{AddressChunk, BlockChunk, CollectError, Erc20Metadata, RowFilter, Source, Table},
    with_series, with_series_binary,
};

#[async_trait::async_trait]
impl Dataset for Erc20Metadata {
    fn datatype(&self) -> Datatype {
        Datatype::Erc20Metadata
    }

    fn name(&self) -> &'static str {
        "erc20_metadata"
    }

    fn column_types(&self) -> HashMap<&'static str, ColumnType> {
        HashMap::from_iter(vec![
            ("block_number", ColumnType::UInt32),
            ("erc20", ColumnType::Binary),
            ("name", ColumnType::String),
            ("symbol", ColumnType::String),
            ("decimals", ColumnType::UInt32),
            ("chain_id", ColumnType::UInt64),
        ])
    }

    fn default_columns(&self) -> Vec<&'static str> {
        vec!["block_number", "erc20", "name", "symbol", "decimals", "chain_id"]
    }

    fn default_sort(&self) -> Vec<String> {
        vec!["symbol".to_string(), "block_number".to_string()]
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

        let rx = fetch_metadata_calls(vec![chunk], contract_chunks, source).await;
        metadata_calls_to_df(rx, schema, source.chain_id).await
    }
}

type MetadataOutput = (u32, Vec<u8>, (Option<Bytes>, Option<Bytes>, Option<Bytes>));

pub(crate) async fn fetch_metadata_calls(
    block_chunks: Vec<&BlockChunk>,
    address_chunks: Vec<AddressChunk>,
    source: &Source,
) -> mpsc::Receiver<Result<MetadataOutput, CollectError>> {
    let (tx, rx) = mpsc::channel(100);

    let name_call_data: Vec<u8> = prefix_hex::decode("0x06fdde03").expect("Decoding failed");
    let symbol_call_data: Vec<u8> = prefix_hex::decode("0x95d89b41").expect("Decoding failed");
    let decimals_call_data: Vec<u8> = prefix_hex::decode("0x313ce567").expect("Decoding failed");

    for block_chunk in block_chunks {
        for number in block_chunk.numbers() {
            for address_chunk in &address_chunks {
                for address in address_chunk.values().iter() {
                    let address = address.clone();
                    let address_h160 = H160::from_slice(&address);

                    let source = source.clone();
                    let name_call_data = name_call_data.clone();
                    let symbol_call_data = symbol_call_data.clone();
                    let decimals_call_data = decimals_call_data.clone();

                    let tx = tx.clone();
                    task::spawn(async move {
                        // name
                        let transaction = TransactionRequest {
                            to: Some(address_h160.into()),
                            data: Some(name_call_data.clone().into()),
                            ..Default::default()
                        };
                        let name_result = source
                            .fetcher
                            .provider
                            .call(&transaction.into(), Some(number.into()))
                            .await
                            .ok();

                        // symbol
                        let transaction = TransactionRequest {
                            to: Some(address_h160.into()),
                            data: Some(symbol_call_data.clone().into()),
                            ..Default::default()
                        };
                        let symbol_result = source
                            .fetcher
                            .provider
                            .call(&transaction.into(), Some(number.into()))
                            .await
                            .ok();

                        // decimals
                        let transaction = TransactionRequest {
                            to: Some(address_h160.into()),
                            data: Some(decimals_call_data.clone().into()),
                            ..Default::default()
                        };
                        let decimals_result = source
                            .fetcher
                            .provider
                            .call(&transaction.into(), Some(number.into()))
                            .await
                            .ok();

                        let result = Ok((
                            number as u32,
                            address,
                            (name_result, symbol_result, decimals_result),
                        ));
                        match tx.send(result).await {
                            Ok(_) => {}
                            Err(tokio::sync::mpsc::error::SendError(_e)) => {
                                eprintln!("send error, try using a rate limit with --requests-per-second or limiting max concurrency with --max-concurrent-requests");
                                std::process::exit(1)
                            }
                        }
                    });
                }
            }
        }
    }

    rx
}

async fn metadata_calls_to_df(
    mut stream: mpsc::Receiver<Result<MetadataOutput, CollectError>>,
    schema: &Table,
    chain_id: u64,
) -> Result<DataFrame, CollectError> {
    // initialize
    let mut columns = Erc20MetadataColumns::default();

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
struct Erc20MetadataColumns {
    n_rows: usize,
    block_number: Vec<u32>,
    erc20: Vec<Vec<u8>>,
    name: Vec<Option<String>>,
    symbol: Vec<Option<String>>,
    decimals: Vec<Option<u32>>,
}

impl Erc20MetadataColumns {
    fn process_calls(&mut self, call_data_output: MetadataOutput, schema: &Table) {
        let (block_number, contract_address, output_data) = call_data_output;
        let (name, symbol, decimals) = output_data;
        self.n_rows += 1;
        if schema.has_column("block_number") {
            self.block_number.push(block_number as u32);
        }
        if schema.has_column("erc20") {
            self.erc20.push(contract_address);
        }
        if schema.has_column("name") {
            let name = match name {
                Some(name) => String::from_utf8(name.to_vec()).ok(),
                None => None,
            };
            self.name.push(name);
        }
        if schema.has_column("symbol") {
            let symbol = match symbol {
                Some(symbol) => String::from_utf8(symbol.to_vec()).ok(),
                None => None,
            };
            self.symbol.push(symbol);
        }
        if schema.has_column("decimals") {
            let decimals = match decimals {
                Some(decimals) => {
                    let v = decimals.to_vec();
                    if v.len() == 32 && v[0..28].iter().all(|b| *b == 0) {
                        Some(u32::from_be_bytes([v[28], v[29], v[30], v[31]]))
                    } else {
                        None
                    }
                }
                None => None,
            };
            self.decimals.push(decimals);
        }
    }

    fn create_df(self, schema: &Table, chain_id: u64) -> Result<DataFrame, CollectError> {
        let mut cols = Vec::with_capacity(schema.columns().len());
        with_series!(cols, "block_number", self.block_number, schema);
        with_series_binary!(cols, "erc20", self.erc20, schema);
        with_series!(cols, "name", self.name, schema);
        with_series!(cols, "symbol", self.symbol, schema);
        with_series!(cols, "decimals", self.decimals, schema);
        with_series!(cols, "chain_id", vec![chain_id; self.n_rows], schema);

        DataFrame::new(cols).map_err(CollectError::PolarsError).sort_by_schema(schema)
    }
}
