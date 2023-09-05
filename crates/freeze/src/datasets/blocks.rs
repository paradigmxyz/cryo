use std::collections::HashMap;

use ethers::prelude::*;
use polars::prelude::*;
use tokio::{sync::mpsc, task};

use crate::{
    dataframes::SortableDataFrame,
    types::{
        conversions::{ToVecHex, ToVecU8},
        BlockChunk, Blocks, CollectError, ColumnType, Dataset, Datatype, RowFilter, Source, Table,
        TransactionChunk,
    },
    with_series, with_series_binary,
};

use super::transactions::TransactionColumns;

pub(crate) type BlockTxGasTuple<TX> = Result<(Block<TX>, Option<Vec<u32>>), CollectError>;

#[async_trait::async_trait]
impl Dataset for Blocks {
    fn datatype(&self) -> Datatype {
        Datatype::Blocks
    }

    fn name(&self) -> &'static str {
        "blocks"
    }

    fn column_types(&self) -> HashMap<&'static str, ColumnType> {
        HashMap::from_iter(vec![
            ("hash", ColumnType::Binary),
            ("parent_hash", ColumnType::Binary),
            ("author", ColumnType::Binary),
            ("state_root", ColumnType::Binary),
            ("transactions_root", ColumnType::Binary),
            ("receipts_root", ColumnType::Binary),
            ("number", ColumnType::UInt32),
            ("gas_used", ColumnType::UInt32),
            ("extra_data", ColumnType::Binary),
            ("logs_bloom", ColumnType::Binary),
            ("timestamp", ColumnType::UInt32),
            ("total_difficulty", ColumnType::String),
            ("size", ColumnType::UInt32),
            ("base_fee_per_gas", ColumnType::UInt64),
            ("chain_id", ColumnType::UInt64),
            // not including: transactions, seal_fields, epoch_snark_data, randomness
        ])
    }

    fn default_columns(&self) -> Vec<&'static str> {
        vec!["number", "hash", "timestamp", "author", "gas_used", "extra_data", "base_fee_per_gas"]
    }

    fn default_sort(&self) -> Vec<String> {
        vec!["number".to_string()]
    }

    async fn collect_block_chunk(
        &self,
        chunk: &BlockChunk,
        source: &Source,
        schema: &Table,
        _filter: Option<&RowFilter>,
    ) -> Result<DataFrame, CollectError> {
        let rx = fetch_blocks(chunk, source).await;
        let output = blocks_to_dfs(rx, &Some(schema), &None, source.chain_id).await;
        match output {
            Ok((Some(blocks_df), _)) => Ok(blocks_df),
            Ok((None, _)) => Err(CollectError::BadSchemaError),
            Err(e) => Err(e),
        }
    }

    async fn collect_transaction_chunk(
        &self,
        chunk: &TransactionChunk,
        source: &Source,
        schema: &Table,
        filter: Option<&RowFilter>,
    ) -> Result<DataFrame, CollectError> {
        let block_numbers = match chunk {
            TransactionChunk::Values(tx_hashes) => {
                fetch_tx_block_numbers(tx_hashes, source).await?
            }
            _ => return Err(CollectError::CollectError("".to_string())),
        };
        let block_chunk = BlockChunk::Numbers(block_numbers);
        self.collect_block_chunk(&block_chunk, source, schema, filter).await
    }
}

async fn fetch_tx_block_numbers(
    tx_hashes: &Vec<Vec<u8>>,
    source: &Source,
) -> Result<Vec<u64>, CollectError> {
    let mut tasks = Vec::new();
    for tx_hash in tx_hashes {
        let fetcher = source.fetcher.clone();
        let tx_hash = tx_hash.clone();
        let task =
            tokio::task::spawn(
                async move { fetcher.get_transaction(H256::from_slice(&tx_hash)).await },
            );
        tasks.push(task);
    }
    let results = futures::future::join_all(tasks).await;
    let mut block_numbers = Vec::new();
    for res in results {
        let tx = res.map_err(|_| CollectError::CollectError("Task join error".to_string()))??;
        match tx {
            Some(transaction) => match transaction.block_number {
                Some(block_number) => block_numbers.push(block_number.as_u64()),
                None => {
                    return Err(CollectError::CollectError("No block number for tx".to_string()))
                }
            },
            None => return Err(CollectError::CollectError("Transaction not found".to_string())),
        }
    }
    block_numbers.sort();
    block_numbers.dedup();
    Ok(block_numbers)
}

async fn fetch_blocks(
    block_chunk: &BlockChunk,
    source: &Source,
) -> mpsc::Receiver<BlockTxGasTuple<TxHash>> {
    let (tx, rx) = mpsc::channel(block_chunk.numbers().len());

    for number in block_chunk.numbers() {
        let tx = tx.clone();
        let fetcher = source.fetcher.clone();
        task::spawn(async move {
            let block = fetcher.get_block(number).await;
            let result = match block {
                Ok(Some(block)) => Ok((block, None)),
                Ok(None) => Err(CollectError::CollectError("block not in node".to_string())),
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
    rx
}

pub(crate) trait ProcessTransactions {
    fn process(&self, schema: &Table, columns: &mut TransactionColumns, gas_used: Option<u32>);
}

impl ProcessTransactions for TxHash {
    fn process(&self, _schema: &Table, _columns: &mut TransactionColumns, _gas_used: Option<u32>) {
        panic!("transaction data not available to process")
    }
}

impl ProcessTransactions for Transaction {
    fn process(&self, schema: &Table, columns: &mut TransactionColumns, gas_used: Option<u32>) {
        columns.process_transaction(self, schema, gas_used)
    }
}

pub(crate) async fn blocks_to_dfs<TX: ProcessTransactions>(
    mut blocks: mpsc::Receiver<BlockTxGasTuple<TX>>,
    blocks_schema: &Option<&Table>,
    transactions_schema: &Option<&Table>,
    chain_id: u64,
) -> Result<(Option<DataFrame>, Option<DataFrame>), CollectError> {
    // initialize
    let mut block_columns = BlockColumns::default();
    let mut transaction_columns = TransactionColumns::default();

    // parse stream of blocks
    while let Some(message) = blocks.recv().await {
        match message {
            Ok((block, gas_used)) => {
                if let Some(schema) = blocks_schema {
                    block_columns.process_block(&block, schema)
                }
                if let Some(schema) = transactions_schema {
                    match gas_used {
                        Some(gas_used) => {
                            for (tx, gas_used) in block.transactions.iter().zip(gas_used) {
                                tx.process(schema, &mut transaction_columns, Some(gas_used))
                            }
                        }
                        None => {
                            for tx in block.transactions.iter() {
                                tx.process(schema, &mut transaction_columns, None)
                            }
                        }
                    }
                }
            }
            Err(e) => {
                println!("{:?}", e);
                return Err(CollectError::TooManyRequestsError)
            }
        }
    }

    // convert to dataframes
    let blocks_df = match blocks_schema {
        Some(schema) => Some(block_columns.create_df(schema, chain_id)?),
        None => None,
    };
    let transactions_df = match transactions_schema {
        Some(schema) => Some(transaction_columns.create_df(schema, chain_id)?),
        None => None,
    };
    Ok((blocks_df, transactions_df))
}

#[derive(Default)]
struct BlockColumns {
    n_rows: usize,
    hash: Vec<Vec<u8>>,
    parent_hash: Vec<Vec<u8>>,
    author: Vec<Vec<u8>>,
    state_root: Vec<Vec<u8>>,
    transactions_root: Vec<Vec<u8>>,
    receipts_root: Vec<Vec<u8>>,
    number: Vec<u32>,
    gas_used: Vec<u32>,
    extra_data: Vec<Vec<u8>>,
    logs_bloom: Vec<Option<Vec<u8>>>,
    timestamp: Vec<u32>,
    total_difficulty: Vec<Option<Vec<u8>>>,
    size: Vec<Option<u32>>,
    base_fee_per_gas: Vec<Option<u64>>,
}

impl BlockColumns {
    fn process_block<TX>(&mut self, block: &Block<TX>, schema: &Table) {
        self.n_rows += 1;
        if schema.has_column("hash") {
            match block.hash {
                Some(h) => self.hash.push(h.as_bytes().to_vec()),
                _ => panic!("invalid block"),
            }
        }
        if schema.has_column("parent_hash") {
            self.parent_hash.push(block.parent_hash.as_bytes().to_vec());
        }
        if schema.has_column("author") {
            match block.author {
                Some(a) => self.author.push(a.as_bytes().to_vec()),
                _ => panic!("invalid block"),
            }
        }
        if schema.has_column("state_root") {
            self.state_root.push(block.state_root.as_bytes().to_vec());
        }
        if schema.has_column("transactions_root") {
            self.transactions_root.push(block.transactions_root.as_bytes().to_vec());
        }
        if schema.has_column("receipts_root") {
            self.receipts_root.push(block.receipts_root.as_bytes().to_vec());
        }
        if schema.has_column("number") {
            match block.number {
                Some(n) => self.number.push(n.as_u32()),
                _ => panic!("invalid block"),
            }
        }
        if schema.has_column("gas_used") {
            self.gas_used.push(block.gas_used.as_u32());
        }
        if schema.has_column("extra_data") {
            self.extra_data.push(block.extra_data.to_vec());
        }
        if schema.has_column("logs_bloom") {
            self.logs_bloom.push(block.logs_bloom.map(|x| x.0.to_vec()));
        }
        if schema.has_column("timestamp") {
            self.timestamp.push(block.timestamp.as_u32());
        }
        if schema.has_column("total_difficulty") {
            self.total_difficulty.push(block.total_difficulty.map(|x| x.to_vec_u8()));
        }
        if schema.has_column("size") {
            self.size.push(block.size.map(|x| x.as_u32()));
        }
        if schema.has_column("base_fee_per_gas") {
            self.base_fee_per_gas.push(block.base_fee_per_gas.map(|value| value.as_u64()));
        }
    }

    fn create_df(self, schema: &Table, chain_id: u64) -> Result<DataFrame, CollectError> {
        let mut cols = Vec::with_capacity(schema.columns().len());
        with_series_binary!(cols, "hash", self.hash, schema);
        with_series_binary!(cols, "parent_hash", self.parent_hash, schema);
        with_series_binary!(cols, "author", self.author, schema);
        with_series_binary!(cols, "state_root", self.state_root, schema);
        with_series_binary!(cols, "transactions_root", self.transactions_root, schema);
        with_series_binary!(cols, "receipts_root", self.receipts_root, schema);
        with_series!(cols, "number", self.number, schema);
        with_series!(cols, "gas_used", self.gas_used, schema);
        with_series_binary!(cols, "extra_data", self.extra_data, schema);
        with_series_binary!(cols, "logs_bloom", self.logs_bloom, schema);
        with_series!(cols, "timestamp", self.timestamp, schema);
        with_series_binary!(cols, "total_difficulty", self.total_difficulty, schema);
        with_series!(cols, "size", self.size, schema);
        with_series!(cols, "base_fee_per_gas", self.base_fee_per_gas, schema);
        with_series!(cols, "chain_id", vec![chain_id; self.n_rows], schema);

        DataFrame::new(cols).map_err(CollectError::PolarsError).sort_by_schema(schema)
    }
}
