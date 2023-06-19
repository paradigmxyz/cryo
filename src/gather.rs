use ethers::prelude::*;
use tokio::sync::Semaphore;
use futures::future::join_all;
use std::sync::Arc;

/// fetch transactions of block_numbers from RPC node
pub async fn get_blocks_txs(
    block_numbers: Vec<u64>,
    provider: Provider<Http>,
    max_concurrent_requests: usize,
) -> Result<Vec<Transaction>, Box<dyn std::error::Error>> {
    let semaphore = Arc::new(Semaphore::new(max_concurrent_requests));

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

    // execute all futures concurrently and collect the results
    let results: Vec<_> = join_all(futures)
        .await
        .into_iter()
        .map(|r| r.unwrap()) // unwrap tokio::task::JoinHandle results
        .collect();

    // Check the results for errors and unwrap successful results.
    // Map transactions from each block into a single, flat vector of transactions.
    let mut all_txs: Vec<Transaction> = Vec::new();
    for result in results {
        let block_with_txs = result?;
        let txs = block_with_txs.unwrap().transactions;
        all_txs.extend(txs);
    }

    Ok(all_txs)
}
