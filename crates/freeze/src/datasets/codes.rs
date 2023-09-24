// required args:: address

use crate::{types::Codes, ColumnType, Dataset, Datatype};
use std::collections::HashMap;

use ethers::prelude::*;
use polars::prelude::*;
use tokio::{sync::mpsc, task};

use ethers::providers::JsonRpcClient;

use crate::{
    dataframes::SortableDataFrame,
    types::{
        conversions::ToVecHex, AddressChunk, BlockChunk, CollectError, RowFilter, Source, Table,
    },
    with_series, with_series_binary,
};

#[async_trait::async_trait]
impl Dataset for Codes {
    fn datatype(&self) -> Datatype {
        Datatype::Codes
    }

    fn name(&self) -> &'static str {
        "codes"
    }

    fn column_types(&self) -> HashMap<&'static str, ColumnType> {
        HashMap::from_iter(vec![
            ("block_number", ColumnType::UInt32),
            ("address", ColumnType::Binary),
            ("code", ColumnType::Binary),
            ("chain_id", ColumnType::UInt64),
        ])
    }

    fn default_columns(&self) -> Vec<&'static str> {
        vec!["block_number", "address", "code"]
    }

    fn default_sort(&self) -> Vec<String> {
        vec!["block_number".to_string(), "address".to_string()]
    }

    async fn collect_block_chunk<P>(
        &self,
        chunk: &BlockChunk,
        source: &Source<P>,
        schema: &Table,
        filter: Option<&RowFilter>,
    ) -> Result<DataFrame, CollectError>
    where
        P: JsonRpcClient,
    {
        let address_chunks = match filter {
            Some(filter) => match &filter.address_chunks {
                Some(address_chunks) => address_chunks.clone(),
                _ => return Err(CollectError::CollectError("must specify addresses".to_string())),
            },
            _ => return Err(CollectError::CollectError("must specify addresses".to_string())),
        };
        let rx = fetch_codes(vec![chunk], address_chunks, source).await;
        codes_to_df(rx, schema, source.chain_id).await
    }
}

pub(crate) type BlockAddressCode = (u64, Vec<u8>, Vec<u8>);

async fn fetch_codes<P>(
    block_chunks: Vec<&BlockChunk>,
    address_chunks: Vec<AddressChunk>,
    source: &Source<P>,
) -> mpsc::Receiver<Result<BlockAddressCode, CollectError>>
where
    P: JsonRpcClient,
{
    let (tx, rx) = mpsc::channel(100);

    for block_chunk in block_chunks {
        for number in block_chunk.numbers() {
            for address_chunk in &address_chunks {
                for address in address_chunk.values().iter() {
                    let address = address.clone();
                    let address_h160 = H160::from_slice(&address);
                    let tx = tx.clone();
                    let source = source.clone();
                    task::spawn(async move {
                        let result = source.fetcher.get_code(address_h160, number.into()).await;
                        let result = match result {
                            Ok(value) => Ok((number, address, value.to_vec())),
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

    rx
}

async fn codes_to_df(
    mut stream: mpsc::Receiver<Result<BlockAddressCode, CollectError>>,
    schema: &Table,
    chain_id: u64,
) -> Result<DataFrame, CollectError> {
    // initialize
    let mut columns = CodeColumns::default();

    // parse stream of blocks
    while let Some(message) = stream.recv().await {
        match message {
            Ok(block_address_code) => {
                columns.process_code(block_address_code, schema);
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
struct CodeColumns {
    n_rows: usize,
    block_number: Vec<u32>,
    address: Vec<Vec<u8>>,
    code: Vec<Vec<u8>>,
}

impl CodeColumns {
    fn process_code(&mut self, block_address_code: BlockAddressCode, schema: &Table) {
        let (block, address, code) = block_address_code;
        self.n_rows += 1;
        if schema.has_column("block_number") {
            self.block_number.push(block as u32);
        }
        if schema.has_column("address") {
            self.address.push(address);
        }
        if schema.has_column("code") {
            self.code.push(code);
        }
    }

    fn create_df(self, schema: &Table, chain_id: u64) -> Result<DataFrame, CollectError> {
        let mut cols = Vec::with_capacity(schema.columns().len());
        with_series!(cols, "block_number", self.block_number, schema);
        with_series_binary!(cols, "address", self.address, schema);
        with_series_binary!(cols, "code", self.code, schema);
        with_series!(cols, "chain_id", vec![chain_id; self.n_rows], schema);

        DataFrame::new(cols).map_err(CollectError::PolarsError).sort_by_schema(schema)
    }
}
