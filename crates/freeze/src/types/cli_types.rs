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

/// Options controling the behavior of a freeze operation
#[derive(Clone)]
pub struct FreezeOpts {
    /// Datatypes to collect
    pub datatypes: Vec<Datatype>,
    // content options
    /// Block chunks to collect
    pub block_chunks: Vec<BlockChunk>,
    /// Schemas for each datatype to collect
    pub schemas: HashMap<Datatype, Schema>,
    // source options
    // pub provider: Provider<Http>,
    /// Provider to use as data source
    pub provider: Arc<Provider<Http>>,
    /// Name of network to use in output files
    pub network_name: String,
    // acquisition options
    /// Rate limiter to use for controlling request rate
    pub rate_limiter: Option<Arc<RateLimiter>>,
    /// Maximum chunks collected concurrently
    pub max_concurrent_chunks: u64,
    /// Maximum concurrent blocks per chunk
    pub max_concurrent_blocks: u64,
    /// Whether to perform a dry run with no actual collection or output
    pub dry_run: bool,
    // output options
    /// Path of directory where to save files
    pub output_dir: String,
    /// Suffix to use at the end of file names
    pub file_suffix: Option<String>,
    /// Whether to overwrite existing files or skip them
    pub overwrite: bool,
    /// File format to used for output files
    pub output_format: FileFormat,
    /// Format to use for binary columns
    pub binary_column_format: ColumnEncoding,
    /// Sorting columns for each dataset
    pub sort: HashMap<Datatype, Vec<String>>,
    /// Number of rows per parquet row group
    pub row_group_size: Option<usize>,
    /// Parquet statistics recording flag
    pub parquet_statistics: bool,
    /// Parquet compression options
    pub parquet_compression: ParquetCompression,
    // dataset-specific options
    // pub gas_used: bool,
    /// Options for collecting logs
    pub log_opts: LogOpts,
}

impl FreezeOpts {
    /// create FetchOpts for an individual chunk
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

/// Summary of freeze operation
pub struct FreezeSummary {
    /// number of chunks completed successfully
    pub n_completed: u64,
    /// number of chunks skipped
    pub n_skipped: u64,
    /// number of chunks that encountered an error
    pub n_errored: u64,
}

/// Summary of freezing a single chunk
pub struct FreezeChunkSummary {
    /// whether chunk was skipped
    pub skipped: bool,
    /// whether chunk encountered an error
    pub errored: bool,
}
