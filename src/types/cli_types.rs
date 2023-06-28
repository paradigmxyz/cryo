use std::collections::HashMap;

use ethers::prelude::*;
use polars::prelude::*;

use crate::types::BlockChunk;
use crate::types::ColumnEncoding;
use crate::types::Datatype;
use crate::types::FileFormat;
use crate::types::Schema;

#[derive(Clone)]
pub struct FreezeOpts {
    pub datatypes: Vec<Datatype>,
    // content options
    pub block_chunks: Vec<BlockChunk>,
    pub schemas: HashMap<Datatype, Schema>,
    // source options
    pub provider: Provider<Http>,
    pub network_name: String,
    // acquisition options
    pub max_concurrent_chunks: u64,
    pub max_concurrent_blocks: u64,
    pub dry_run: bool,
    // output options
    pub output_dir: String,
    pub overwrite: bool,
    pub output_format: FileFormat,
    pub binary_column_format: ColumnEncoding,
    pub sort: HashMap<Datatype, Vec<String>>,
    pub row_groups: Option<u64>,
    pub row_group_size: Option<u64>,
    pub parquet_statistics: bool,
    pub compression: ParquetCompression,
    // dataset-specific options
    pub gas_used: bool,
    pub contract: Option<H160>,
    pub topic0: Option<H256>,
    pub topic1: Option<H256>,
    pub topic2: Option<H256>,
    pub topic3: Option<H256>,
    pub log_request_size: u64,
}

pub struct FreezeSummary {
    pub n_completed: u64,
    pub n_skipped: u64,
}

pub struct FreezeChunkSummary {
    pub skipped: bool,
}

#[derive(Debug)]
pub enum CompressionParseError {
    InvalidCompressionAlgorithm,
    InvalidCompressionLevel,
    MissingCompressionLevel,
}

