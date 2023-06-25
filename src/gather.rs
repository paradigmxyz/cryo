use ethers::prelude::*;
use futures::future::join_all;
use std::sync::Arc;
use tokio::sync::Semaphore;

use crate::chunks;
use crate::types::{FreezeOpts, SlimBlock, BlockChunk};

pub async fn get_blocks_and_transactions(
    block_numbers: Vec<u64>,
    opts: &FreezeOpts,
) -> Result<(Vec<SlimBlock>, Vec<Transaction>), Box<dyn std::error::Error>> {
    let results =
        fetch_blocks_and_transactions(block_numbers, &opts.provider, &opts.max_concurrent_blocks);

    let mut blocks: Vec<SlimBlock> = Vec::new();
    let mut txs: Vec<Transaction> = Vec::new();
    for result in results.await.unwrap() {
        match result {
            Some(block) => {
                let slim_block = chunks::block_to_slim_block(&block);
                blocks.push(slim_block);
                let block_txs = block.transactions;
                txs.extend(block_txs);
            }
            _ => {}
        }
    }

    Ok((blocks, txs))
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

pub async fn get_transactions(
    block_numbers: Vec<u64>,
    opts: &FreezeOpts,
) -> Result<Vec<Transaction>, Box<dyn std::error::Error>> {
    let results =
        fetch_blocks_and_transactions(block_numbers, &opts.provider, &opts.max_concurrent_blocks);

    let mut txs: Vec<Transaction> = Vec::new();
    for result in results.await.unwrap() {
        match result {
            Some(block) => {
                let block_txs = block.transactions;
                txs.extend(block_txs);
            }
            _ => {}
        }
    }

    Ok(txs)
}

pub async fn fetch_blocks_and_transactions(
    block_numbers: Vec<u64>,
    provider: &Provider<Http>,
    max_concurrent_blocks: &u64
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

pub async fn get_logs(
    block_chunk: &BlockChunk,
    address: Option<ValueOrArray<H160>>,
    topics: [Option<ValueOrArray<Option<H256>>>; 4],
    opts: &FreezeOpts,
) -> Result<Vec<Log>, Box<dyn std::error::Error>> {
    let request_chunks = chunks::block_chunk_to_filter_options(
        &block_chunk,
        &opts.log_request_size,
    );
    let results = fetch_logs(
        request_chunks,
        address,
        topics,
        &opts.provider,
        &opts.max_concurrent_blocks,
    ).await.unwrap();

    let mut logs: Vec<Log> = Vec::new();
    for result in results {
        for log in result.unwrap() {
            logs.push(log);
        }
    }

    Ok(logs)
}

pub async fn fetch_logs(
    request_chunks: Vec<FilterBlockOption>,
    address: Option<ValueOrArray<H160>>,
    topics: [Option<ValueOrArray<Option<H256>>>; 4],
    provider: &Provider<Http>,
    max_concurrent_requests: &u64,
) -> Result<Vec<Option<Vec<Log>>>, Box<dyn std::error::Error>> {
    let semaphore = Arc::new(Semaphore::new(*max_concurrent_requests as usize));

    // prepare futures for concurrent execution
    let futures = request_chunks.into_iter().map(|request_chunk| {
        let provider = provider.clone();
        let semaphore = Arc::clone(&semaphore); // Cloning the Arc, not the Semaphore
        let filter = Filter {
            block_option: request_chunk,
            address: address.clone(),
            topics: topics.clone(),
        };
        tokio::spawn(async move {
            let permit = Arc::clone(&semaphore).acquire_owned().await;
            let result = provider.get_logs(&filter).await;
            drop(permit); // release the permit when the task is done
            result
        })
    });

    let results: Result<Vec<Option<Vec<Log>>>, _> = join_all(futures)
    .await
    .into_iter()
    .map(|r| match r {
        Ok(Ok(block)) => Ok(Some(block)),
        Ok(Err(e)) => {
            println!("Failed to get block: {}", e);
            Ok(None)
        }
        Err(e) => Err(format!("Task failed: {}", e)),
    })
    .collect();

    match results {
        Ok(blocks) => Ok(blocks),
        Err(e) => Err(Box::from(e)), // Convert the error into a boxed dyn Error
    }
}
