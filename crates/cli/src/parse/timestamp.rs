use ethers::prelude::*;
use polars::prelude::*;
use cryo_freeze::{BlockChunk, Fetcher, ParseError, ChunkData, CollectError};

use crate::Args;

pub async fn timestamp_to_block_number<P: JsonRpcClient>(timestamp: u64, args: &Args, fetcher: Arc<Fetcher<P>>) -> Result<u64, CollectError> {
    let mut block_numbers: Vec<u64> = Vec::new();
    let latest_block_number = get_latest_block_number(args, fetcher.clone()).await?;

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
    args: &Args,
    fetcher: Arc<Fetcher<P>>
) -> Result<u64, ParseError> {
    let (_, block_numbers) = crate::parse::blocks::parse_blocks(args, fetcher.clone()).await?;
    let block_numbers = block_numbers.unwrap();
    let latest_block_numbers = block_numbers[block_numbers.len() - 1].values();

    return Ok(latest_block_numbers[latest_block_numbers.len() - 1]);
}