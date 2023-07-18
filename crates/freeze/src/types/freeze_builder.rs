use std::collections::HashMap;
use std::sync::Arc;

use ethers::prelude::*;
use polars::prelude::*;

use super::freezes::FreezeOpts;
use crate::types::Chunk;
use crate::types::ColumnEncoding;
use crate::types::Datatype;
use crate::types::FileFormat;
use crate::types::LogOpts;
use crate::types::RateLimiter;
use crate::types::Table;

/// Builder struct for FreezeOpts
pub struct FreezeOptsBuilder {
    datatypes: Option<Vec<Datatype>>,
    chunks: Option<Vec<Chunk>>,
    schemas: Option<HashMap<Datatype, Table>>,
    provider: Option<Arc<Provider<Http>>>,
    chain_id: Option<u64>,
    network_name: Option<String>,
    rate_limiter: Option<Arc<RateLimiter>>,
    max_concurrent_chunks: Option<u64>,
    max_concurrent_blocks: Option<u64>,
    dry_run: Option<bool>,
    output_dir: Option<String>,
    file_suffix: Option<String>,
    overwrite: Option<bool>,
    output_format: Option<FileFormat>,
    binary_column_format: Option<ColumnEncoding>,
    row_group_size: Option<usize>,
    parquet_statistics: Option<bool>,
    parquet_compression: Option<ParquetCompression>,
    log_opts: Option<LogOpts>,
}

impl Default for FreezeOptsBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl FreezeOptsBuilder {
    /// create new FreezeOptsBuilder
    pub fn new() -> FreezeOptsBuilder {
        FreezeOptsBuilder {
            datatypes: None,
            chunks: None,
            schemas: None,
            provider: None,
            chain_id: None,
            network_name: None,
            rate_limiter: None,
            max_concurrent_chunks: None,
            max_concurrent_blocks: None,
            dry_run: None,
            output_dir: None,
            file_suffix: None,
            overwrite: None,
            output_format: None,
            binary_column_format: None,
            row_group_size: None,
            parquet_statistics: None,
            parquet_compression: None,
            log_opts: None,
        }
    }

    /// datatypes field of FreezeOptsBuilder
    pub fn datatypes(&mut self, datatypes: Vec<Datatype>) -> &mut Self {
        self.datatypes = Some(datatypes);
        self
    }

    /// chunks field of FreezeOptsBuilder
    pub fn chunks(&mut self, chunks: Vec<Chunk>) -> &mut Self {
        self.chunks = Some(chunks);
        self
    }

    /// schemas field of FreezeOptsBuilder
    pub fn schemas(&mut self, schemas: HashMap<Datatype, Table>) -> &mut Self {
        self.schemas = Some(schemas);
        self
    }

    /// provider field of FreezeOptsBuilder
    pub fn provider(&mut self, provider: Arc<Provider<Http>>) -> &mut Self {
        self.provider = Some(provider);
        self
    }

    /// chain_id field of FreezeOptsBuilder
    pub fn chain_id(&mut self, chain_id: u64) -> &mut Self {
        self.chain_id = Some(chain_id);
        self
    }

    /// network_name field of FreezeOptsBuilder
    pub fn network_name(&mut self, network_name: String) -> &mut Self {
        self.network_name = Some(network_name);
        self
    }

    /// rate_limiter field of FreezeOptsBuilder
    pub fn rate_limiter(&mut self, rate_limiter: Arc<RateLimiter>) -> &mut Self {
        self.rate_limiter = Some(rate_limiter);
        self
    }

    /// max_concurrent_chunks field of FreezeOptsBuilder
    pub fn max_concurrent_chunks(&mut self, max_concurrent_chunks: u64) -> &mut Self {
        self.max_concurrent_chunks = Some(max_concurrent_chunks);
        self
    }

    /// max_concurrent_blocks field of FreezeOptsBuilder
    pub fn max_concurrent_blocks(&mut self, max_concurrent_blocks: u64) -> &mut Self {
        self.max_concurrent_blocks = Some(max_concurrent_blocks);
        self
    }

    /// dry_run field of FreezeOptsBuilder
    pub fn dry_run(&mut self, dry_run: bool) -> &mut Self {
        self.dry_run = Some(dry_run);
        self
    }

    /// output_dir field of FreezeOptsBuilder
    pub fn output_dir(&mut self, output_dir: String) -> &mut Self {
        self.output_dir = Some(output_dir);
        self
    }

    /// file_suffix field of FreezeOptsBuilder
    pub fn file_suffix(&mut self, file_suffix: String) -> &mut Self {
        self.file_suffix = Some(file_suffix);
        self
    }

    /// overwrite field of FreezeOptsBuilder
    pub fn overwrite(&mut self, overwrite: bool) -> &mut Self {
        self.overwrite = Some(overwrite);
        self
    }

    /// output_format field of FreezeOptsBuilder
    pub fn output_format(&mut self, output_format: FileFormat) -> &mut Self {
        self.output_format = Some(output_format);
        self
    }

    /// binary_column_format field of FreezeOptsBuilder
    pub fn binary_column_format(&mut self, binary_column_format: ColumnEncoding) -> &mut Self {
        self.binary_column_format = Some(binary_column_format);
        self
    }

    /// row_group_size field of FreezeOptsBuilder
    pub fn row_group_size(&mut self, row_group_size: usize) -> &mut Self {
        self.row_group_size = Some(row_group_size);
        self
    }

    /// parquet_statistics field of FreezeOptsBuilder
    pub fn parquet_statistics(&mut self, parquet_statistics: bool) -> &mut Self {
        self.parquet_statistics = Some(parquet_statistics);
        self
    }

    /// parquet_compression field of FreezeOptsBuilder
    pub fn parquet_compression(&mut self, parquet_compression: ParquetCompression) -> &mut Self {
        self.parquet_compression = Some(parquet_compression);
        self
    }

    /// log_opts field of FreezeOptsBuilder
    pub fn log_opts(&mut self, log_opts: LogOpts) -> &mut Self {
        self.log_opts = Some(log_opts);
        self
    }

    /// build FreezeOpts
    pub fn build(self) -> Result<FreezeOpts, &'static str> {
        if let (
            Some(datatypes),
            Some(chunks),
            Some(schemas),
            Some(provider),
            Some(chain_id),
            Some(network_name),
            rate_limiter,
            Some(max_concurrent_chunks),
            Some(max_concurrent_blocks),
            Some(dry_run),
            Some(output_dir),
            file_suffix,
            Some(overwrite),
            Some(output_format),
            Some(binary_column_format),
            row_group_size,
            Some(parquet_statistics),
            Some(parquet_compression),
            Some(log_opts),
        ) = (
            self.datatypes,
            self.chunks,
            self.schemas,
            self.provider,
            self.chain_id,
            self.network_name,
            self.rate_limiter,
            self.max_concurrent_chunks,
            self.max_concurrent_blocks,
            self.dry_run,
            self.output_dir,
            self.file_suffix,
            self.overwrite,
            self.output_format,
            self.binary_column_format,
            self.row_group_size,
            self.parquet_statistics,
            self.parquet_compression,
            self.log_opts,
        ) {
            Ok(FreezeOpts {
                datatypes,
                chunks,
                schemas,
                provider,
                chain_id,
                network_name,
                rate_limiter,
                max_concurrent_chunks,
                max_concurrent_blocks,
                dry_run,
                output_dir,
                file_suffix,
                overwrite,
                output_format,
                binary_column_format,
                row_group_size,
                parquet_statistics,
                parquet_compression,
                log_opts,
            })
        } else {
            Err("All fields need to be set")
        }
    }
}
