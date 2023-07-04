use std::collections::HashMap;
use std::sync::Arc;

use ethers::prelude::*;
use polars::prelude::*;

use crate::types::BlockChunk;
use crate::types::ColumnEncoding;
use crate::types::Datatype;
use crate::types::FetchOpts;
use crate::types::FileFormat;
use crate::types::LogOpts;
use crate::types::RateLimiter;
use crate::types::Schema;

#[derive(Clone)]
pub struct FreezeOpts {
    pub datatypes: Vec<Datatype>,
    // content options
    pub block_chunks: Vec<BlockChunk>,
    pub schemas: HashMap<Datatype, Schema>,
    // source options
    // pub provider: Provider<Http>,
    pub provider: Arc<Provider<Http>>,
    pub network_name: String,
    // acquisition options
    pub rate_limiter: Option<Arc<RateLimiter>>,
    pub max_concurrent_chunks: u64,
    pub max_concurrent_blocks: u64,
    pub dry_run: bool,
    // output options
    pub output_dir: String,
    pub file_suffix: Option<String>,
    pub overwrite: bool,
    pub output_format: FileFormat,
    pub binary_column_format: ColumnEncoding,
    pub sort: HashMap<Datatype, Vec<String>>,
    pub row_group_size: Option<usize>,
    pub parquet_statistics: bool,
    pub parquet_compression: ParquetCompression,
    // dataset-specific options
    // pub gas_used: bool,
    pub log_opts: LogOpts,
}

impl FreezeOpts {
    pub fn chunk_fetch_opts(&self) -> FetchOpts {
        let sem = Arc::new(tokio::sync::Semaphore::new(
            self.max_concurrent_blocks as usize,
        ));
        FetchOpts {
            // provider: self.provider.clone(),
            provider: Arc::clone(&self.provider),
            rate_limiter: self.rate_limiter.as_ref().map(Arc::clone),
            semaphore: sem,
        }
    }
}

pub struct FreezeSummary {
    pub n_completed: u64,
    pub n_skipped: u64,
    pub n_errored: u64,
}

pub struct FreezeChunkSummary {
    pub skipped: bool,
    pub errored: bool,
}
