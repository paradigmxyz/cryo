use crate::{conversions::ToVecHex, types::EthCalls, ColumnType, Dataset, Datatype};
use std::collections::HashMap;
use tokio::{sync::mpsc, task};

use ethers::prelude::*;
use polars::prelude::*;

use crate::{
    dataframes::SortableDataFrame,
    types::{AddressChunk, BlockChunk, CallDataChunk, CollectError, RowFilter, Source, Table},
    with_series, with_series_binary,
};

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

    async fn collect_block_chunk(
        &self,
        chunk: &BlockChunk,
        source: &Source,
        schema: &Table,
        filter: Option<&RowFilter>,
    ) -> Result<DataFrame, CollectError> {
        let (address_chunks, call_data_chunks) = match filter {
            Some(filter) => (filter.address_chunks()?, filter.call_data_chunks()?),
            _ => return Err(CollectError::CollectError("must specify RowFilter".to_string())),
        };
        let rx = fetch_eth_calls(vec![chunk], address_chunks, call_data_chunks, source).await;
        eth_calls_to_df(rx, schema, source.chain_id).await
    }
}

// block, address, call_data, output
pub(crate) type CallDataOutput = (u64, Vec<u8>, Vec<u8>, Bytes);

pub(crate) async fn fetch_eth_calls(
    block_chunks: Vec<&BlockChunk>,
    address_chunks: Vec<AddressChunk>,
    call_data_chunks: Vec<CallDataChunk>,
    source: &Source,
) -> mpsc::Receiver<Result<CallDataOutput, CollectError>> {
    let (tx, rx) = mpsc::channel(100);

    for block_chunk in block_chunks {
        for number in block_chunk.numbers() {
            for address_chunk in &address_chunks {
                for address in address_chunk.values().iter() {
                    for call_data_chunk in &call_data_chunks {
                        for call_data in call_data_chunk.values().iter() {
                            let address = address.clone();
                            let address_h160 = H160::from_slice(&address);
                            let call_data = call_data.clone();

                            let tx = tx.clone();
                            let source = source.clone();
                            task::spawn(async move {
                                let transaction = TransactionRequest {
                                    to: Some(address_h160.into()),
                                    data: Some(call_data.clone().into()),
                                    ..Default::default()
                                };

                                let result = source.fetcher.call(transaction, number.into()).await;
                                let result = match result {
                                    Ok(value) => Ok((number, address, call_data, value)),
                                    Err(e) => Err(e),
                                };
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
        }
    }

    rx
}

async fn eth_calls_to_df(
    mut stream: mpsc::Receiver<Result<CallDataOutput, CollectError>>,
    schema: &Table,
    chain_id: u64,
) -> Result<DataFrame, CollectError> {
    // initialize
    let mut columns = CallDataColumns::default();

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
struct CallDataColumns {
    n_rows: usize,
    block_number: Vec<u32>,
    contract_address: Vec<Vec<u8>>,
    call_data: Vec<Vec<u8>>,
    call_data_hash: Vec<Vec<u8>>,
    output_data: Vec<Vec<u8>>,
    output_data_hash: Vec<Vec<u8>>,
}

impl CallDataColumns {
    fn process_calls(&mut self, call_data_output: CallDataOutput, schema: &Table) {
        let (block_number, contract_address, call_data, output_data) = call_data_output;
        self.n_rows += 1;
        if schema.has_column("block_number") {
            self.block_number.push(block_number as u32);
        }
        if schema.has_column("contract_address") {
            self.contract_address.push(contract_address);
        }
        if schema.has_column("call_data_hash") {
            let call_data_hash = ethers_core::utils::keccak256(call_data.clone()).into();
            self.call_data_hash.push(call_data_hash);
        }
        if schema.has_column("call_data") {
            self.call_data.push(call_data);
        }
        if schema.has_column("output_data") {
            self.output_data.push(output_data.to_vec());
        }
        if schema.has_column("output_data_hash") {
            let output_data_hash: Vec<u8> = ethers_core::utils::keccak256(output_data).into();
            self.output_data.push(output_data_hash.to_vec());
        }
    }

    fn create_df(self, schema: &Table, chain_id: u64) -> Result<DataFrame, CollectError> {
        let mut cols = Vec::with_capacity(schema.columns().len());
        with_series!(cols, "block_number", self.block_number, schema);
        with_series_binary!(cols, "contract_address", self.contract_address, schema);
        with_series_binary!(cols, "call_data", self.call_data, schema);
        with_series_binary!(cols, "call_data_hash", self.call_data_hash, schema);
        with_series_binary!(cols, "output_data", self.output_data, schema);
        with_series_binary!(cols, "output_data_hash", self.output_data_hash, schema);
        with_series!(cols, "chain_id", vec![chain_id; self.n_rows], schema);

        DataFrame::new(cols).map_err(CollectError::PolarsError).sort_by_schema(schema)
    }
}
