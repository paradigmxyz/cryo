use ethers::prelude::*;
use polars::prelude::*;
use cryo_freeze::{BlockChunk, Fetcher, ParseError, ChunkData};

use crate::Args;

pub async fn timestamp_to_block<P: JsonRpcClient>(timestamp: u32, fetcher: &Fetcher<P>) -> Result<u64, ParseError> {
    todo!()
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