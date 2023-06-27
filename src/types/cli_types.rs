use std::collections::HashMap;

use ethers::prelude::*;

use crate::types::BlockChunk;
use crate::types::ColumnEncoding;
use crate::types::Datatype;
use crate::types::FileFormat;
use crate::types::Schema;

#[derive(Clone)]
pub struct FreezeOpts {
    pub datatypes: Vec<Datatype>,
    pub provider: Provider<Http>,
    pub block_chunks: Vec<BlockChunk>,
    pub network_name: String,
    pub max_concurrent_chunks: u64,
    pub max_concurrent_blocks: u64,
    pub log_request_size: u64,
    pub binary_column_format: ColumnEncoding,
    pub schemas: HashMap<Datatype, Schema>,
    pub output_dir: String,
    pub output_format: FileFormat,
    pub dry_run: bool,
    pub sort: HashMap<Datatype, Vec<String>>,
    pub row_groups: Option<u64>,
    pub row_group_size: Option<u64>,
    pub parquet_statistics: bool,
    pub overwrite: bool,
}

pub struct FreezeSummary {
    pub n_completed: u64,
    pub n_skipped: u64,
}

pub struct FreezeChunkSummary {
    pub skipped: bool,
}

