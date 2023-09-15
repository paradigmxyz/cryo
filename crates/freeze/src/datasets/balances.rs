// required args:: address

use crate::{types::Balances, ColumnType, Dataset, Datatype};
use std::collections::HashMap;

use ethers::prelude::*;
use polars::prelude::*;
use tokio::{sync::mpsc, task};

use crate::{
    dataframes::SortableDataFrame,
    types::{
        conversions::{ToVecHex, ToVecU8},
        AddressChunk, BlockChunk, CollectError, RowFilter, Source, Table,
    },
    with_series, with_series_binary, with_series_u256, ColumnEncoding, U256Type,
};

#[async_trait::async_trait]
impl Dataset for Balances {
    fn datatype(&self) -> Datatype {
        Datatype::Balances
    }

    fn name(&self) -> &'static str {
        "balances"
    }

    fn column_types(&self) -> HashMap<&'static str, ColumnType> {
        HashMap::from_iter(vec![
            ("block_number", ColumnType::UInt32),
            ("address", ColumnType::Binary),
            ("balance", ColumnType::UInt256),
            ("chain_id", ColumnType::UInt64),
        ])
    }

    fn default_columns(&self) -> Vec<&'static str> {
        vec!["block_number", "address", "balance", "chain_id"]
    }

    fn default_sort(&self) -> Vec<String> {
        vec!["block_number".to_string(), "address".to_string()]
    }

    async fn collect_block_chunk(
        &self,
        chunk: &BlockChunk,
        source: &Source,
        schema: &Table,
        filter: Option<&RowFilter>,
    ) -> Result<DataFrame, CollectError> {
        let address_chunks = match filter {
            Some(filter) => match &filter.address_chunks {
                Some(address_chunks) => address_chunks.clone(),
                _ => return Err(CollectError::CollectError("must specify addresses".to_string())),
            },
            _ => return Err(CollectError::CollectError("must specify addresses".to_string())),
        };
        let rx = fetch_balances(vec![chunk], address_chunks, source).await;
        balances_to_df(rx, schema, source.chain_id).await
    }
}

pub(crate) type BlockAddressBalance = (u64, Vec<u8>, U256);

async fn fetch_balances(
    block_chunks: Vec<&BlockChunk>,
    address_chunks: Vec<AddressChunk>,
    source: &Source,
) -> mpsc::Receiver<Result<BlockAddressBalance, CollectError>> {
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
                        let balance = source.fetcher.get_balance(address_h160, number.into()).await;
                        let result = match balance {
                            Ok(value) => Ok((number, address, value)),
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

async fn balances_to_df(
    mut stream: mpsc::Receiver<Result<BlockAddressBalance, CollectError>>,
    schema: &Table,
    chain_id: u64,
) -> Result<DataFrame, CollectError> {
    // initialize
    let mut columns = BalanceColumns::default();

    // parse stream of blocks
    while let Some(message) = stream.recv().await {
        match message {
            Ok(block_address_balance) => {
                columns.process_balance(block_address_balance, schema);
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
struct BalanceColumns {
    n_rows: usize,
    block_number: Vec<u32>,
    address: Vec<Vec<u8>>,
    balance: Vec<U256>,
}

impl BalanceColumns {
    fn process_balance(&mut self, block_address_balance: BlockAddressBalance, schema: &Table) {
        let (block, address, balance) = block_address_balance;
        self.n_rows += 1;
        if schema.has_column("block_number") {
            self.block_number.push(block as u32);
        }
        if schema.has_column("address") {
            self.address.push(address);
        }
        if schema.has_column("balance") {
            self.balance.push(balance);
        }
    }

    fn create_df(self, schema: &Table, chain_id: u64) -> Result<DataFrame, CollectError> {
        let mut cols = Vec::with_capacity(schema.columns().len());
        with_series!(cols, "block_number", self.block_number, schema);
        with_series_binary!(cols, "address", self.address, schema);
        with_series_u256!(cols, "balance", self.balance, schema);
        with_series!(cols, "chain_id", vec![chain_id; self.n_rows], schema);

        DataFrame::new(cols).map_err(CollectError::PolarsError).sort_by_schema(schema)
    }
}
