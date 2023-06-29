use std::collections::HashMap;
use std::collections::HashSet;
use std::sync::Arc;

use ethers::prelude::*;
use futures::future::join_all;
use polars::prelude::*;
use tokio::sync::Semaphore;

use crate::chunks;
use crate::types::BlockChunk;
use crate::types::Blocks;
use crate::types::ColumnType;
use crate::types::Dataset;
use crate::types::Datatype;
use crate::types::FreezeOpts;
use crate::types::Schema;

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
            ("block_number", ColumnType::Int32),
            ("block_hash", ColumnType::Binary),
            ("timestamp", ColumnType::Int32),
            ("author", ColumnType::Binary),
            ("gas_used", ColumnType::Int32),
            ("extra_data", ColumnType::Binary),
            ("base_fee_per_gas", ColumnType::Int64),
        ])
    }

    fn default_columns(&self) -> Vec<&'static str> {
        vec![
            "block_number",
            "block_hash",
            "timestamp",
            "author",
            "gas_used",
            "extra_data",
            "base_fee_per_gas",
        ]
    }

    fn default_sort(&self) -> Vec<String> {
        vec!["block_number".to_string()]
    }

    async fn collect_chunk(&self, block_chunk: &BlockChunk, opts: &FreezeOpts) -> DataFrame {
        let numbers = chunks::get_chunk_block_numbers(block_chunk);
        let blocks = fetch_blocks(numbers, &opts.provider, &opts.max_concurrent_blocks)
            .await
            .unwrap();
        let blocks = blocks.into_iter().flatten().collect();
        blocks_to_df(blocks, &opts.schemas[&Datatype::Blocks]).unwrap()
    }

    // async fn collect_chunk_with_extras(
    //     &self,
    //     block_chunk: &BlockChunk,
    //     extras: &HashSet<Datatype>,
    //     opts: &FreezeOpts,
    // ) -> HashMap<Datatype, DataFrame> {
    //     if extras.is_empty() {
    //         let df = self.collect_chunk(block_chunk, opts).await;
    //         [(Datatype::Blocks, df)].iter().cloned().collect()
    //     } else if (extras.len() == 1) & extras.contains(&Datatype::Transactions) {
    //         let numbers = chunks::get_chunk_block_numbers(block_chunk);
    //         let blocks = fetch_blocks(numbers, &opts.provider, &opts.max_concurrent_blocks)
    //             .await
    //             .unwrap();
    //         let blocks = blocks.into_iter().flatten().collect();
    //         let blocks = blocks_to_df(blocks, &opts.schemas[&Datatype::Blocks]).unwrap()
    //         [(Datatype::Blocks, blocks), (Datatype::Transactions, transactions)].iter().cloned().collect()
    //     } else {
    //         panic!("invalid extras")
    //     }
    // }

}

pub async fn fetch_blocks(
    block_numbers: Vec<u64>,
    provider: &Provider<Http>,
    max_concurrent_blocks: &u64,
) -> Result<Vec<Option<Block<TxHash>>>, Box<dyn std::error::Error>> {
    let semaphore = Arc::new(Semaphore::new(*max_concurrent_blocks as usize));

    // prepare futures for concurrent execution
    let futures = block_numbers.into_iter().map(|block_number| {
        let provider = provider.clone();
        let semaphore = Arc::clone(&semaphore); // Cloning the Arc, not the Semaphore
        tokio::spawn(async move {
            let permit = Arc::clone(&semaphore).acquire_owned().await;
            let result = provider.get_block(block_number).await;
            drop(permit); // release the permit when the task is done
            result
        })
    });

    let results: Result<Vec<Option<Block<TxHash>>>, _> = join_all(futures)
        .await
        .into_iter()
        .map(|r| match r {
            Ok(Ok(block)) => Ok(block),
            Ok(Err(e)) => {
                println!("Failed to get block: {}", e);
                Ok(None)
            }
            Err(e) => Err(format!("Task failed: {}", e)),
        })
        .collect();

    match results {
        Ok(blocks) => Ok(blocks),
        Err(e) => Err(e.into()), // Convert the error into a boxed dyn Error
    }
}

pub async fn fetch_blocks_and_transactions(
    block_numbers: Vec<u64>,
    provider: &Provider<Http>,
    max_concurrent_blocks: &u64,
) -> Result<Vec<Option<Block<Transaction>>>, Box<dyn std::error::Error>> {
    let semaphore = Arc::new(Semaphore::new(*max_concurrent_blocks as usize));

    // prepare futures for concurrent execution
    let futures = block_numbers.into_iter().map(|block_number| {
        let provider = provider.clone();
        let semaphore = Arc::clone(&semaphore); // Cloning the Arc, not the Semaphore
        tokio::spawn(async move {
            let permit = Arc::clone(&semaphore).acquire_owned().await;
            let result = provider.get_block_with_txs(block_number).await;
            drop(permit); // release the permit when the task is done
            result
        })
    });

    let results: Result<Vec<Option<Block<Transaction>>>, _> = join_all(futures)
        .await
        .into_iter()
        .map(|r| match r {
            Ok(Ok(block)) => Ok(block),
            Ok(Err(e)) => {
                println!("Failed to get block: {}", e);
                Ok(None)
            }
            Err(e) => Err(format!("Task failed: {}", e)),
        })
        .collect();

    match results {
        Ok(blocks) => Ok(blocks),
        Err(e) => Err(e.into()), // Convert the error into a boxed dyn Error
    }
}

pub fn blocks_to_df(blocks: Vec<Block<TxHash>>, schema: &Schema) -> Result<DataFrame, PolarsError> {
    let include_number = schema.contains_key("block_number");
    let include_hash = schema.contains_key("block_hash");
    let include_author = schema.contains_key("author");
    let include_gas_used = schema.contains_key("gas_used");
    let include_extra_data = schema.contains_key("extra_data");
    let include_timestamp = schema.contains_key("timestamp");
    let include_base_fee_per_gas = schema.contains_key("base_fee_per_gas");

    let mut number: Vec<u64> = Vec::with_capacity(blocks.len());
    let mut hash: Vec<Vec<u8>> = Vec::with_capacity(blocks.len());
    let mut author: Vec<Vec<u8>> = Vec::with_capacity(blocks.len());
    let mut gas_used: Vec<u64> = Vec::with_capacity(blocks.len());
    let mut extra_data: Vec<Vec<u8>> = Vec::with_capacity(blocks.len());
    let mut timestamp: Vec<u64> = Vec::with_capacity(blocks.len());
    let mut base_fee_per_gas: Vec<Option<u64>> = Vec::with_capacity(blocks.len());

    for block in blocks.iter() {
        if let (Some(n), Some(h), Some(a)) = (block.number, block.hash, block.author) {
            if include_number {
                number.push(n.as_u64())
            }
            if include_hash {
                hash.push(h.as_bytes().to_vec());
            }
            if include_author {
                author.push(a.as_bytes().to_vec());
            }
            if include_gas_used {
                gas_used.push(block.gas_used.as_u64());
            }
            if include_extra_data {
                extra_data.push(block.extra_data.to_vec());
            }
            if include_timestamp {
                timestamp.push(block.timestamp.as_u64());
            }
            if include_base_fee_per_gas {
                base_fee_per_gas.push(block.base_fee_per_gas.map(|value| value.as_u64()));
            }
        }
    }

    let mut cols = Vec::new();
    if include_number {
        cols.push(Series::new("block_number", number));
    };
    if include_hash {
        cols.push(Series::new("block_hash", hash));
    };
    if include_author {
        cols.push(Series::new("author", author));
    };
    if include_gas_used {
        cols.push(Series::new("gas_used", gas_used));
    };
    if include_extra_data {
        cols.push(Series::new("extra_data", extra_data));
    };
    if include_timestamp {
        cols.push(Series::new("timestamp", timestamp));
    };
    if include_base_fee_per_gas {
        cols.push(Series::new("base_fee_per_gas", base_fee_per_gas));
    };

    DataFrame::new(cols)
}
