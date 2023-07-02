use std::collections::HashMap;
use std::sync::Arc;

use ethers::prelude::*;
use futures::future::join_all;
use polars::prelude::*;
use tokio::sync::Semaphore;

use crate::chunks;
use crate::types::conversions::ToVecHex;
use crate::types::conversions::ToVecU8;
use crate::types::BlockChunk;
use crate::types::Blocks;
use crate::types::CollectError;
use crate::types::ColumnType;
use crate::types::Dataset;
use crate::types::Datatype;
use crate::types::FreezeOpts;
use crate::types::Schema;
use crate::with_series;
use crate::with_series_binary;

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
            ("number", ColumnType::Int32),
            ("gas_used", ColumnType::Int32),
            ("extra_data", ColumnType::Binary),
            ("logs_bloom", ColumnType::Binary),
            ("timestamp", ColumnType::Int32),
            ("total_difficulty", ColumnType::String),
            ("size", ColumnType::Int32),
            ("base_fee_per_gas", ColumnType::Int64),
            // not including: transactions, seal_fields, epoch_snark_data, randomness
        ])
    }

    fn default_columns(&self) -> Vec<&'static str> {
        vec![
            "number",
            "hash",
            "timestamp",
            "author",
            "gas_used",
            "extra_data",
            "base_fee_per_gas",
        ]
    }

    fn default_sort(&self) -> Vec<String> {
        vec!["number".to_string()]
    }

    async fn collect_chunk(
        &self,
        block_chunk: &BlockChunk,
        opts: &FreezeOpts,
    ) -> Result<DataFrame, CollectError> {
        let numbers = chunks::get_chunk_block_numbers(block_chunk);
        let blocks = fetch_blocks(numbers, &opts.provider, &opts.max_concurrent_blocks).await?;
        let blocks = blocks.into_iter().flatten().collect();
        let df = blocks_to_df(blocks, &opts.schemas[&Datatype::Blocks])
            .map_err(CollectError::PolarsError);
        if let Some(sort_keys) = opts.sort.get(&Datatype::Blocks) {
            df.map(|x| x.sort(sort_keys, false))?
                .map_err(CollectError::PolarsError)
        } else {
            df
        }
    }
}

pub async fn fetch_blocks(
    numbers: Vec<u64>,
    provider: &Provider<Http>,
    max_concurrent_blocks: &u64,
) -> Result<Vec<Option<Block<TxHash>>>, CollectError> {
    let semaphore = Arc::new(Semaphore::new(*max_concurrent_blocks as usize));

    let futures = numbers.into_iter().map(|number| {
        let provider = provider.clone();
        let semaphore = Arc::clone(&semaphore);
        tokio::spawn(async move {
            let _permit = Arc::clone(&semaphore).acquire_owned().await;
            provider.get_block(number).await
        })
    });

    join_all(futures)
        .await
        .into_iter()
        .map(|r| match r {
            Ok(Ok(block)) => Ok(block),
            Ok(Err(e)) => Err(CollectError::ProviderError(e)),
            Err(e) => Err(CollectError::TaskFailed(e)),
        })
        .collect()
}

pub async fn fetch_blocks_and_transactions(
    numbers: Vec<u64>,
    provider: &Provider<Http>,
    max_concurrent_blocks: &u64,
) -> Result<Vec<Option<Block<Transaction>>>, CollectError> {
    let semaphore = Arc::new(Semaphore::new(*max_concurrent_blocks as usize));

    let futures = numbers.into_iter().map(|number| {
        let provider = provider.clone();
        let semaphore = Arc::clone(&semaphore);
        tokio::spawn(async move {
            let _permit = Arc::clone(&semaphore).acquire_owned().await;
            provider.get_block_with_txs(number).await
        })
    });

    join_all(futures)
        .await
        .into_iter()
        .map(|r| match r {
            Ok(Ok(block)) => Ok(block),
            Ok(Err(e)) => Err(CollectError::ProviderError(e)),
            Err(e) => Err(CollectError::TaskFailed(e)),
        })
        .collect()
}

pub fn blocks_to_df(blocks: Vec<Block<TxHash>>, schema: &Schema) -> Result<DataFrame, PolarsError> {
    let include_hash = schema.contains_key("hash");
    let include_parent_hash = schema.contains_key("parent_hash");
    let include_author = schema.contains_key("author");
    let include_state_root = schema.contains_key("state_root");
    let include_transactions_root = schema.contains_key("transactions_root");
    let include_receipts_root = schema.contains_key("receipts_root");
    let include_number = schema.contains_key("number");
    let include_gas_used = schema.contains_key("gas_used");
    let include_extra_data = schema.contains_key("extra_data");
    let include_logs_bloom = schema.contains_key("logs_bloom");
    let include_timestamp = schema.contains_key("timestamp");
    let include_total_difficulty = schema.contains_key("total_difficulty");
    let include_size = schema.contains_key("size");
    let include_base_fee_per_gas = schema.contains_key("base_fee_per_gas");

    let capacity = blocks.len();
    let mut hash: Vec<Vec<u8>> = Vec::with_capacity(capacity);
    let mut parent_hash: Vec<Vec<u8>> = Vec::with_capacity(capacity);
    let mut author: Vec<Vec<u8>> = Vec::with_capacity(capacity);
    let mut state_root: Vec<Vec<u8>> = Vec::with_capacity(capacity);
    let mut transactions_root: Vec<Vec<u8>> = Vec::with_capacity(capacity);
    let mut receipts_root: Vec<Vec<u8>> = Vec::with_capacity(capacity);
    let mut number: Vec<u32> = Vec::with_capacity(capacity);
    let mut gas_used: Vec<u64> = Vec::with_capacity(capacity);
    let mut extra_data: Vec<Vec<u8>> = Vec::with_capacity(capacity);
    let mut logs_bloom: Vec<Option<Vec<u8>>> = Vec::with_capacity(capacity);
    let mut timestamp: Vec<u64> = Vec::with_capacity(capacity);
    let mut total_difficulty: Vec<Option<Vec<u8>>> = Vec::with_capacity(capacity);
    let mut size: Vec<Option<u64>> = Vec::with_capacity(capacity);
    let mut base_fee_per_gas: Vec<Option<u64>> = Vec::with_capacity(capacity);

    for block in blocks.iter() {
        if let (Some(n), Some(h), Some(a)) = (block.number, block.hash, block.author) {
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
                gas_used.push(block.gas_used.as_u64());
            }
            if include_extra_data {
                extra_data.push(block.extra_data.to_vec());
            }
            if include_logs_bloom {
                logs_bloom.push(block.logs_bloom.map(|x| x.0.to_vec()));
            }
            if include_timestamp {
                timestamp.push(block.timestamp.as_u64());
            }
            if include_total_difficulty {
                total_difficulty.push(block.total_difficulty.map(|x| x.to_vec_u8()));
            }
            if include_size {
                size.push(block.size.map(|x| x.as_u64()));
            }
            if include_base_fee_per_gas {
                base_fee_per_gas.push(block.base_fee_per_gas.map(|value| value.as_u64()));
            }
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
    DataFrame::new(cols)
}
