use ethers::prelude::*;
use cryo_freeze::{BlockChunk, Fetcher, ParseError, ChunkData};

pub async fn timestamp_to_block<P: JsonRpcClient>(timestamp: u32, fetcher: &Fetcher<P>) -> Result<u64, ParseError> {
    todo!()
}