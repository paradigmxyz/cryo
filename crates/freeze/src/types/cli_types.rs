use std::collections::HashMap;
use std::sync::Arc;

use ethers::prelude::*;
use polars::prelude::*;

use crate::types::Chunk;
use crate::types::ColumnEncoding;
use crate::types::Datatype;
use crate::types::FetchOpts;
use crate::types::FileFormat;
use crate::types::LogOpts;
use crate::types::RateLimiter;
use crate::types::Table;

/// Options controling the behavior of a freeze operation
#[derive(Clone)]
pub struct FreezeOpts {
    /// Datatypes to collect
    pub datatypes: Vec<Datatype>,
    // content options
    /// Block chunks to collect
    pub chunks: Vec<Chunk>,
    /// Schemas for each datatype to collect
    pub schemas: HashMap<Datatype, Table>,
    // source options
    /// Provider to use as data source
    pub provider: Arc<Provider<Http>>,
    /// Chain id of network
    pub chain_id: u64,
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

pub(crate) trait FreezeSummaryAgg {
    fn aggregate(self) -> FreezeSummary;
}

impl FreezeSummaryAgg for Vec<FreezeChunkSummary> {
    fn aggregate(self) -> FreezeSummary {
        let mut n_completed: u64 = 0;
        let mut n_skipped: u64 = 0;
        let mut n_errored: u64 = 0;

        for chunk_summary in self {
            if chunk_summary.skipped {
                n_skipped += 1;
            } else if chunk_summary.errored {
                n_errored += 1;
            } else {
                n_completed += 1;
            }
        }

        FreezeSummary {
            n_completed,
            n_skipped,
            n_errored,
        }
    }
}

/// Summary of freezing a single chunk
pub struct FreezeChunkSummary {
    /// whether chunk was skipped
    pub skipped: bool,
    /// whether chunk encountered an error
    pub errored: bool,
}

impl FreezeChunkSummary {
    pub(crate) fn success() -> FreezeChunkSummary {
        FreezeChunkSummary {
            skipped: false,
            errored: true,
        }
    }

    pub(crate) fn error() -> FreezeChunkSummary {
        FreezeChunkSummary {
            skipped: false,
            errored: true,
        }
    }

    pub(crate) fn skip() -> FreezeChunkSummary {
        FreezeChunkSummary {
            skipped: true,
            errored: false,
        }
    }
}
