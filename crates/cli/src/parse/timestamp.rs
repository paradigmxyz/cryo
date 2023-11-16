use ethers::prelude::*;
use polars::prelude::*;
use cryo_freeze::{BlockChunk, Fetcher, ParseError, ChunkData, CollectError};

async fn timestamp_to_block_number<P: JsonRpcClient>(
    timestamp: u64,
    fetcher: &Fetcher<P>
) -> Result<u64, CollectError> {
    let mut block_numbers: Vec<u64> = Vec::new();
    let latest_block_number = get_latest_block_number(fetcher).await?;

    for block_number in 0..latest_block_number {
        block_numbers.push(block_number);
    }

    // perform binary search to determine the block number for a given timestamp.
    // If the exact timestamp is not found, we return the closest block number.
    let mut l = 0;
    let mut r = block_numbers.len() as u64 - 1;
    let mut mid = l + (r - 1) / 2;
    let mut block = fetcher.get_block(mid).await?.unwrap();

    while l <= r {
        mid = l + (r - 1) / 2;
        block = fetcher.get_block(mid).await?.unwrap();

        if block.timestamp == timestamp.into() {
            return Ok(mid);
        } else if block.timestamp < timestamp.into() {
            l = mid + 1;
        } else {
            r = mid - 1;
        }
    }

    return Ok(mid);
}

async fn get_latest_block_number<P: JsonRpcClient>(
    fetcher: &Fetcher<P>,
) -> Result<u64, ParseError> {
    return fetcher
        .get_block_number()
        .await
        .map(|n| n.as_u64())
        .map_err(|_e| ParseError::ParseError("Error retrieving latest block number".to_string()));
}