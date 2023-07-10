use std::{collections::HashMap, sync::Arc};

use ethers::prelude::*;
use polars::prelude::*;
use tokio::{sync::mpsc, task};

use crate::{
    chunks::ChunkAgg,
    dataframes::SortableDataFrame,
    types::{
        conversions::{ToVecHex, ToVecU8},
        BlockChunk, Blocks, CollectError, ColumnType, Dataset, Datatype, FetchOpts, FreezeOpts,
        Table,
    },
    with_series, with_series_binary,
};

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

    async fn collect_chunk(
        &self,
        block_chunk: &BlockChunk,
        opts: &FreezeOpts,
    ) -> Result<DataFrame, CollectError> {
        let rx = fetch_blocks(block_chunk, &opts.chunk_fetch_opts()).await;
        blocks_to_df(rx, &opts.schemas[&Datatype::Blocks], opts.chain_id).await
    }
}

async fn fetch_blocks(
    block_chunk: &BlockChunk,
    opts: &FetchOpts,
) -> mpsc::Receiver<Result<Option<Block<TxHash>>, CollectError>> {
    let (tx, rx) = mpsc::channel(block_chunk.numbers().len());

    for number in block_chunk.numbers() {
        let tx = tx.clone();
        let provider = opts.provider.clone();
        let semaphore = opts.semaphore.clone();
        let rate_limiter = opts.rate_limiter.as_ref().map(Arc::clone);
        task::spawn(async move {
            let _permit = Arc::clone(&semaphore).acquire_owned().await;
            if let Some(limiter) = rate_limiter {
                Arc::clone(&limiter).until_ready().await;
            }
            let block = provider.get_block(number).await.map_err(CollectError::ProviderError);
            match tx.send(block).await {
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

async fn blocks_to_df(
    mut blocks: mpsc::Receiver<Result<Option<Block<TxHash>>, CollectError>>,
    schema: &Table,
    chain_id: u64,
) -> Result<DataFrame, CollectError> {
    let include_hash = schema.has_column("hash");
    let include_parent_hash = schema.has_column("parent_hash");
    let include_author = schema.has_column("author");
    let include_state_root = schema.has_column("state_root");
    let include_transactions_root = schema.has_column("transactions_root");
    let include_receipts_root = schema.has_column("receipts_root");
    let include_number = schema.has_column("number");
    let include_gas_used = schema.has_column("gas_used");
    let include_extra_data = schema.has_column("extra_data");
    let include_logs_bloom = schema.has_column("logs_bloom");
    let include_timestamp = schema.has_column("timestamp");
    let include_total_difficulty = schema.has_column("total_difficulty");
    let include_size = schema.has_column("size");
    let include_base_fee_per_gas = schema.has_column("base_fee_per_gas");

    let capacity = 0;
    let mut hash: Vec<Vec<u8>> = Vec::with_capacity(capacity);
    let mut parent_hash: Vec<Vec<u8>> = Vec::with_capacity(capacity);
    let mut author: Vec<Vec<u8>> = Vec::with_capacity(capacity);
    let mut state_root: Vec<Vec<u8>> = Vec::with_capacity(capacity);
    let mut transactions_root: Vec<Vec<u8>> = Vec::with_capacity(capacity);
    let mut receipts_root: Vec<Vec<u8>> = Vec::with_capacity(capacity);
    let mut number: Vec<u32> = Vec::with_capacity(capacity);
    let mut gas_used: Vec<u32> = Vec::with_capacity(capacity);
    let mut extra_data: Vec<Vec<u8>> = Vec::with_capacity(capacity);
    let mut logs_bloom: Vec<Option<Vec<u8>>> = Vec::with_capacity(capacity);
    let mut timestamp: Vec<u32> = Vec::with_capacity(capacity);
    let mut total_difficulty: Vec<Option<Vec<u8>>> = Vec::with_capacity(capacity);
    let mut size: Vec<Option<u32>> = Vec::with_capacity(capacity);
    let mut base_fee_per_gas: Vec<Option<u64>> = Vec::with_capacity(capacity);

    let mut n_rows = 0;
    while let Some(message) = blocks.recv().await {
        match message {
            Ok(Some(block)) => {
                if let (Some(n), Some(h), Some(a)) = (block.number, block.hash, block.author) {
                    n_rows += 1;

                    if include_hash {
                        hash.push(h.as_bytes().to_vec());
                    }
                    if include_parent_hash {
                        parent_hash.push(block.parent_hash.as_bytes().to_vec());
                    }
                    if include_author {
                        author.push(a.as_bytes().to_vec());
                    }
                    if include_state_root {
                        state_root.push(block.state_root.as_bytes().to_vec());
                    }
                    if include_transactions_root {
                        transactions_root.push(block.transactions_root.as_bytes().to_vec());
                    }
                    if include_receipts_root {
                        receipts_root.push(block.receipts_root.as_bytes().to_vec());
                    }
                    if include_number {
                        number.push(n.as_u32())
                    }
                    if include_gas_used {
                        gas_used.push(block.gas_used.as_u32());
                    }
                    if include_extra_data {
                        extra_data.push(block.extra_data.to_vec());
                    }
                    if include_logs_bloom {
                        logs_bloom.push(block.logs_bloom.map(|x| x.0.to_vec()));
                    }
                    if include_timestamp {
                        timestamp.push(block.timestamp.as_u32());
                    }
                    if include_total_difficulty {
                        total_difficulty.push(block.total_difficulty.map(|x| x.to_vec_u8()));
                    }
                    if include_size {
                        size.push(block.size.map(|x| x.as_u32()));
                    }
                    if include_base_fee_per_gas {
                        base_fee_per_gas.push(block.base_fee_per_gas.map(|value| value.as_u64()));
                    }
                }
            }
            _ => return Err(CollectError::TooManyRequestsError),
        }
    }

    let mut cols = Vec::new();
    with_series_binary!(cols, "hash", hash, schema);
    with_series_binary!(cols, "parent_hash", parent_hash, schema);
    with_series_binary!(cols, "author", author, schema);
    with_series_binary!(cols, "state_root", state_root, schema);
    with_series_binary!(cols, "transactions_root", transactions_root, schema);
    with_series_binary!(cols, "receipts_root", receipts_root, schema);
    with_series!(cols, "number", number, schema);
    with_series!(cols, "gas_used", gas_used, schema);
    with_series_binary!(cols, "extra_data", extra_data, schema);
    with_series_binary!(cols, "logs_bloom", logs_bloom, schema);
    with_series!(cols, "timestamp", timestamp, schema);
    with_series_binary!(cols, "total_difficulty", total_difficulty, schema);
    with_series!(cols, "size", size, schema);
    with_series!(cols, "base_fee_per_gas", base_fee_per_gas, schema);

    if schema.has_column("chain_id") {
        cols.push(Series::new("chain_id", vec![chain_id; n_rows]));
    }

    DataFrame::new(cols).map_err(CollectError::PolarsError).sort_by_schema(schema)
}
