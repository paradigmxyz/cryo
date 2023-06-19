use ethers::prelude::*;
use futures::future::join_all;
use std::sync::Arc;
use tokio::sync::Semaphore;

pub struct SlimBlock {
    pub number: u64,
    pub hash: Vec<u8>,
    pub author: Vec<u8>,
    pub gas_used: u64,
    pub extra_data: Vec<u8>,
    pub timestamp: u64,
    pub base_fee_per_gas: Option<u64>,
}

fn block_to_slim_block<T>(block: &Block<T>) -> SlimBlock {
    SlimBlock {
        number: block.number.unwrap().as_u64(),
        hash: block.hash.unwrap().as_bytes().to_vec(),
        author: block.author.unwrap().as_bytes().to_vec(),
        gas_used: block.gas_used.as_u64(),
        extra_data: block.extra_data.to_vec(),
        timestamp: block.timestamp.as_u64(),
        base_fee_per_gas: block.base_fee_per_gas.map(|value| value.as_u64()),
    }
}

pub async fn get_blocks_and_transactions(
    block_numbers: Vec<u64>,
    provider: &Provider<Http>,
    max_concurrent_requests: &Option<usize>,
) -> Result<(Vec<SlimBlock>, Vec<Transaction>), Box<dyn std::error::Error>> {
    let results = fetch_blocks_and_transactions(block_numbers, &provider, &max_concurrent_requests);

    let mut blocks: Vec<SlimBlock> = Vec::new();
    let mut txs: Vec<Transaction> = Vec::new();
    for result in results.await.unwrap() {
        match result {
            Some(block) => {
                let slim_block = block_to_slim_block(&block);
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
    provider: &Provider<Http>,
    max_concurrent_requests: &Option<usize>,
) -> Result<Vec<SlimBlock>, Box<dyn std::error::Error>> {
    let results = fetch_blocks(block_numbers, &provider, &max_concurrent_requests);

    let mut blocks: Vec<SlimBlock> = Vec::new();
    for result in results.await.unwrap() {
        match result {
            Some(block) => {
                let slim_block = block_to_slim_block(&block);
                blocks.push(slim_block);
            }
            _ => {}
        }
    }

    Ok(blocks)
}

pub async fn get_transactions(
    block_numbers: Vec<u64>,
    provider: &Provider<Http>,
    max_concurrent_requests: &Option<usize>,
) -> Result<Vec<Transaction>, Box<dyn std::error::Error>> {
    let results = fetch_blocks_and_transactions(block_numbers, &provider, &max_concurrent_requests);

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
    max_concurrent_requests: &Option<usize>,
) -> Result<Vec<Option<Block<Transaction>>>, Box<dyn std::error::Error>> {
    let semaphore = Arc::new(Semaphore::new(max_concurrent_requests.unwrap_or(100)));

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
    max_concurrent_requests: &Option<usize>,
) -> Result<Vec<Option<Block<TxHash>>>, Box<dyn std::error::Error>> {
    let semaphore = Arc::new(Semaphore::new(max_concurrent_requests.unwrap_or(100)));

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
