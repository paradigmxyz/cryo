use ethers::prelude::*;

pub struct FreezeOpts {
    pub datatype: String,
    pub provider: Provider<Http>,
    pub max_concurrent_requests: Option<usize>,
    pub start_block: Option<u64>,
    pub end_block: Option<u64>,
    pub block_numbers: Option<Vec<u64>>,
    pub chunk_size: u64,
    pub network_name: String,
    pub log_request_size: u64,
}

#[derive(Default)]
pub struct BlockChunk {
    pub start_block: Option<u64>,
    pub end_block: Option<u64>,
    pub block_numbers: Option<Vec<u64>>,
}

pub struct SlimBlock {
    pub number: u64,
    pub hash: Vec<u8>,
    pub author: Vec<u8>,
    pub gas_used: u64,
    pub extra_data: Vec<u8>,
    pub timestamp: u64,
    pub base_fee_per_gas: Option<u64>,
}
