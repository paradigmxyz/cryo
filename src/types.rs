use ethers::prelude::*;

#[derive(Clone)]
pub struct FreezeOpts {
    pub datatype: String,
    pub provider: Provider<Http>,
    pub block_chunks: Vec<BlockChunk>,
    pub network_name: String,
    pub max_concurrent_chunks: u64,
    pub max_concurrent_blocks: u64,
    pub log_request_size: u64,
    pub output_dir: String,
    pub output_format: String,
    pub binary_column_format: String,
}

#[derive(Default)]
#[derive(Clone)]
pub struct BlockChunk {
    pub start_block: Option<u64>,
    pub end_block: Option<u64>,
    pub block_numbers: Option<Vec<u64>>,
}

#[derive(Default)]
pub struct SlimBlock {
    pub number: u64,
    pub hash: Vec<u8>,
    pub author: Vec<u8>,
    pub gas_used: u64,
    pub extra_data: Vec<u8>,
    pub timestamp: u64,
    pub base_fee_per_gas: Option<u64>,
}
