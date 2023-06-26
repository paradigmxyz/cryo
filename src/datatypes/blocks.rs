use std::collections::HashMap;

use ethers::prelude::*;
use futures::future::join_all;
use polars::prelude::*;
use std::sync::Arc;
use tokio::sync::Semaphore;

use crate::chunks;
use crate::types::Blocks;
use crate::types::BlockChunk;
use crate::types::ColumnType;
use crate::types::Dataset;
use crate::types::FreezeOpts;
use crate::types::SlimBlock;

#[async_trait::async_trait]
impl Dataset for Blocks {
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

    async fn collect_dataset(&self, block_chunk: &BlockChunk, opts: &FreezeOpts) -> DataFrame {
        let block_numbers = chunks::get_chunk_block_numbers(&block_chunk);
        let blocks = get_blocks(block_numbers, &opts).await.unwrap();
        blocks_to_df(blocks).unwrap()
    }
}

pub async fn get_blocks(
    block_numbers: Vec<u64>,
    opts: &FreezeOpts,
) -> Result<Vec<SlimBlock>, Box<dyn std::error::Error>> {
    let results = fetch_blocks(block_numbers, &opts.provider, &opts.max_concurrent_blocks);

    let mut blocks: Vec<SlimBlock> = Vec::new();
    for result in results.await.unwrap() {
        match result {
            Some(block) => {
                let slim_block = chunks::block_to_slim_block(&block);
                blocks.push(slim_block);
            }
            _ => {}
        }
    }

    Ok(blocks)
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

// pub async fn get_blocks_and_transactions(
//     block_numbers: Vec<u64>,
//     opts: &FreezeOpts,
// ) -> Result<(Vec<SlimBlock>, Vec<Transaction>), Box<dyn std::error::Error>> {
//     let results =
//         fetch_blocks_and_transactions(block_numbers, &opts.provider, &opts.max_concurrent_blocks);

//     let mut blocks: Vec<SlimBlock> = Vec::new();
//     let mut txs: Vec<Transaction> = Vec::new();
//     for result in results.await.unwrap() {
//         match result {
//             Some(block) => {
//                 let slim_block = chunks::block_to_slim_block(&block);
//                 blocks.push(slim_block);
//                 let block_txs = block.transactions;
//                 txs.extend(block_txs);
//             }
//             _ => {}
//         }
//     }

//     Ok((blocks, txs))
// }

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

pub fn blocks_to_df(blocks: Vec<SlimBlock>) -> Result<DataFrame, Box<dyn std::error::Error>> {
    let mut number: Vec<u64> = Vec::new();
    let mut hash: Vec<Vec<u8>> = Vec::new();
    let mut author: Vec<Vec<u8>> = Vec::new();
    let mut gas_used: Vec<u64> = Vec::new();
    let mut extra_data: Vec<Vec<u8>> = Vec::new();
    let mut timestamp: Vec<u64> = Vec::new();
    let mut base_fee_per_gas: Vec<Option<u64>> = Vec::new();

    for block in blocks.iter() {
        number.push(block.number);
        hash.push(block.hash.clone());
        author.push(block.author.clone());
        gas_used.push(block.gas_used);
        extra_data.push(block.extra_data.clone());
        timestamp.push(block.timestamp);
        base_fee_per_gas.push(block.base_fee_per_gas);
    }

    let df = df!(
        "block_number" => number,
        "block_hash" => hash,
        "timestamp" => timestamp,
        "author" => author,
        "gas_used" => gas_used,
        "extra_data" => extra_data,
        "base_fee_per_gas" => base_fee_per_gas,
    );

    Ok(df?)
}
