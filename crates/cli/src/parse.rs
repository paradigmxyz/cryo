use std::collections::HashMap;
use std::env;
use std::fs;

use clap::Parser;
use ethers::prelude::*;
use eyre::Result;
use eyre::WrapErr;
use governor::Quota;
use governor::RateLimiter;
use hex::FromHex;
use polars::prelude::*;
use std::num::NonZeroU32;

use cryo_freeze::BlockChunk;
use cryo_freeze::ChunkAgg;
use cryo_freeze::ChunkOps;
use cryo_freeze::ColumnEncoding;
use cryo_freeze::Datatype;
use cryo_freeze::FileFormat;
use cryo_freeze::FreezeOpts;
use cryo_freeze::Schema;

use crate::args::Args;

async fn apply_reorg_buffer(
    block_chunks: Vec<BlockChunk>,
    reorg_filter: u64,
    provider: &Provider<Http>,
) -> Result<Vec<BlockChunk>, ProviderError> {
    match reorg_filter {
        0 => Ok(block_chunks),
        reorg_filter => {
            let latest_block = provider.get_block_number().await?.as_u64();
            let max_allowed = latest_block - reorg_filter;
            Ok(block_chunks
                .into_iter()
                .filter_map(|x| match x.max_block() {
                    Some(max_block) if max_block <= max_allowed => Some(x),
                    _ => None,
                })
                .collect())
        }
    }
}

/// parse options for running freeze
pub async fn parse_opts() -> Result<FreezeOpts> {
    // parse args
    let args = Args::parse();

    // parse datatypes
    let datatypes: Result<Vec<Datatype>, _> = args
        .datatype
        .iter()
        .map(|datatype| parse_datatype(datatype))
        .collect();
    let datatypes = datatypes?;

    // parse network info
    let rpc_url = parse_rpc_url(&args);
    let provider = Provider::<Http>::try_from(rpc_url)?;
    let network_name = match &args.network_name {
        Some(name) => name.clone(),
        None => match provider.get_chainid().await {
            Ok(chain_id) => match chain_id.as_u64() {
                1 => "ethereum".to_string(),
                chain_id => "network_".to_string() + chain_id.to_string().as_str(),
            },
            _ => return Err(eyre::eyre!("could not determine chain_id")),
        },
    };

    // parse block chunks
    let block_chunks = parse_block_inputs(&args.blocks, &provider).await?;
    let block_chunks = if args.align {
        block_chunks
            .into_iter()
            .filter_map(|x| x.align(args.chunk_size))
            .collect()
    } else {
        block_chunks
    };
    let block_chunks = match args.n_chunks {
        Some(n_chunks) => block_chunks.subchunk_by_count(&n_chunks),
        None => block_chunks.subchunk_by_size(&args.chunk_size),
    };
    let block_chunks = apply_reorg_buffer(block_chunks, args.reorg_buffer, &provider).await?;

    // process output directory
    let output_dir = std::fs::canonicalize(args.output_dir.clone())
        .wrap_err("Failed to canonicalize output directory")?
        .to_string_lossy()
        .into_owned();
    match fs::create_dir_all(&output_dir) {
        Ok(_) => {}
        Err(e) => return Err(eyre::eyre!(format!("Error creating directory: {}", e))),
    };

    // process output formats
    let output_format = match (args.csv, args.json) {
        (true, true) => return Err(eyre::eyre!("choose one of parquet, csv, or json")),
        (true, _) => FileFormat::Csv,
        (_, true) => FileFormat::Json,
        (false, false) => FileFormat::Parquet,
    };
    let binary_column_format = match args.hex | (output_format != FileFormat::Parquet) {
        true => ColumnEncoding::Hex,
        false => ColumnEncoding::Binary,
    };

    // process concurrency info
    let (max_concurrent_chunks, max_concurrent_blocks) = parse_concurrency_args(&args)?;

    // process schemas
    let schemas: Result<HashMap<Datatype, Schema>, eyre::Report> = datatypes
        .iter()
        .map(|datatype| {
            datatype
                .get_schema(
                    &binary_column_format,
                    &args.include_columns,
                    &args.exclude_columns,
                )
                .map(|schema| (*datatype, schema))
                .wrap_err_with(|| format!("Failed to get schema for datatype: {:?}", datatype))
        })
        .collect();
    let schemas = schemas?;

    let sort = parse_sort(&args.sort, &schemas)?;

    let contract = parse_address(&args.contract);
    let topic0 = parse_topic(&args.topic0);
    let topic1 = parse_topic(&args.topic1);
    let topic2 = parse_topic(&args.topic2);
    let topic3 = parse_topic(&args.topic3);

    let parquet_compression = parse_compression(&args.compression)?;

    let row_group_size = parse_row_group_size(
        args.row_group_size,
        args.n_row_groups,
        block_chunks.get(0).map(|x| x.total_blocks() as usize),
    );

    let rate_limiter = match args.requests_per_second {
        Some(rate_limit) => match NonZeroU32::new(rate_limit) {
            Some(value) => {
                let quota = Quota::per_second(value);
                Some(Arc::new(RateLimiter::direct(quota)))
            }
            _ => None,
        },
        None => None,
    };

    // compile opts
    let opts = FreezeOpts {
        datatypes,
        // content options
        block_chunks,
        schemas,
        // source options
        provider,
        network_name,
        // acquisition options
        rate_limiter,
        max_concurrent_chunks,
        max_concurrent_blocks,
        dry_run: args.dry,
        // output options
        output_dir,
        overwrite: args.overwrite,
        output_format,
        binary_column_format,
        sort,
        row_group_size,
        parquet_statistics: !args.no_stats,
        parquet_compression,
        // dataset-specific options
        // gas_used: args.gas_used,
        contract,
        topic0,
        topic1,
        topic2,
        topic3,
        log_request_size: args.log_request_size,
    };
    Ok(opts)
}

fn parse_datatype(datatype: &str) -> Result<Datatype> {
    let datatype = match datatype {
        "balance_diffs" => Datatype::BalanceDiffs,
        "blocks" => Datatype::Blocks,
        "code_diffs" => Datatype::CodeDiffs,
        "logs" => Datatype::Logs,
        "events" => Datatype::Logs,
        "nonce_diffs" => Datatype::NonceDiffs,
        "storage_diffs" => Datatype::StorageDiffs,
        "transactions" => Datatype::Transactions,
        "txs" => Datatype::Transactions,
        "traces" => Datatype::Traces,
        "vm_traces" => Datatype::VmTraces,
        _ => return Err(eyre::eyre!("invalid datatype {}", datatype)),
    };
    Ok(datatype)
}

pub fn parse_row_group_size(
    row_group_size: Option<usize>,
    n_row_groups: Option<usize>,
    chunk_size: Option<usize>,
) -> Option<usize> {
    match (row_group_size, n_row_groups, chunk_size) {
        (Some(row_group_size), _, _) => Some(row_group_size),
        (_, Some(n_row_groups), Some(cs)) => Some((cs + n_row_groups - 1) / n_row_groups),
        _ => None,
    }
}

pub fn parse_rpc_url(args: &Args) -> String {
    let mut url = match &args.rpc {
        Some(url) => url.clone(),
        _ => match env::var("ETH_RPC_URL") {
            Ok(url) => url,
            Err(_e) => {
                println!("must provide --rpc or set ETH_RPC_URL");
                std::process::exit(0);
            }
        },
    };
    if !url.starts_with("http") {
        url = "http://".to_string() + url.as_str();
    };
    url
}

fn parse_address(input: &Option<String>) -> Option<ValueOrArray<H160>> {
    input.as_ref().and_then(|data| {
        <[u8; 20]>::from_hex(data.as_str().chars().skip(2).collect::<String>().as_str())
            .ok()
            .map(H160)
            .map(ValueOrArray::Value)
    })
}

fn parse_topic(input: &Option<String>) -> Option<ValueOrArray<Option<H256>>> {
    let value = input.as_ref().and_then(|data| {
        <[u8; 32]>::from_hex(data.as_str().chars().skip(2).collect::<String>().as_str())
            .ok()
            .map(H256)
    });

    value.map(|inner| ValueOrArray::Value(Some(inner)))
}

fn parse_compression(input: &Vec<String>) -> Result<ParquetCompression> {
    match input.as_slice() {
        [algorithm] if algorithm.as_str() == "uncompressed" => Ok(ParquetCompression::Uncompressed),
        [algorithm] if algorithm.as_str() == "snappy" => Ok(ParquetCompression::Snappy),
        [algorithm] if algorithm.as_str() == "lzo" => Ok(ParquetCompression::Lzo),
        [algorithm] if algorithm.as_str() == "lz4" => Ok(ParquetCompression::Lz4Raw),
        [algorithm, level_str] if algorithm.as_str() == "gzip" => match level_str.parse::<u8>() {
            Ok(level) => match GzipLevel::try_new(level) {
                Ok(gzip_level) => Ok(ParquetCompression::Gzip(Some(gzip_level))),
                Err(_) => Err(eyre::eyre!("Invalid compression level")),
            },
            Err(_) => Err(eyre::eyre!("Invalid compression level")),
        },
        [algorithm, level_str] if algorithm.as_str() == "brotli" => {
            match level_str.parse::<u32>() {
                Ok(level) => match BrotliLevel::try_new(level) {
                    Ok(brotli_level) => Ok(ParquetCompression::Brotli(Some(brotli_level))),
                    Err(_) => Err(eyre::eyre!("Invalid compression level")),
                },
                Err(_) => Err(eyre::eyre!("Invalid compression level")),
            }
        }
        [algorithm, level_str] if algorithm.as_str() == "zstd" => match level_str.parse::<i32>() {
            Ok(level) => match ZstdLevel::try_new(level) {
                Ok(zstd_level) => Ok(ParquetCompression::Zstd(Some(zstd_level))),
                Err(_) => Err(eyre::eyre!("Invalid compression level")),
            },
            Err(_) => Err(eyre::eyre!("Invalid compression level")),
        },
        [algorithm] if ["gzip", "brotli", "zstd"].contains(&algorithm.as_str()) => {
            Err(eyre::eyre!("Missing compression level"))
        }
        _ => Err(eyre::eyre!("Invalid compression algorithm")),
    }
}

fn parse_sort(
    raw_sort: &Vec<String>,
    schemas: &HashMap<Datatype, Schema>,
) -> Result<HashMap<Datatype, Vec<String>>, eyre::Report> {
    if raw_sort.is_empty() {
        Ok(HashMap::from_iter(schemas.iter().map(
            |(datatype, _schema)| (*datatype, datatype.dataset().default_sort()),
        )))
    } else if schemas.len() > 1 {
        Err(eyre::eyre!(
            "custom sort not supported for multiple schemas"
        ))
    } else {
        match schemas.keys().next() {
            Some(datatype) => Ok(HashMap::from_iter([(*datatype, raw_sort.clone())])),
            None => Err(eyre::eyre!("schemas map is empty")),
        }
    }
}

fn parse_concurrency_args(args: &Args) -> Result<(u64, u64)> {
    let result = match (
        args.max_concurrent_requests,
        args.max_concurrent_chunks,
        args.max_concurrent_blocks,
    ) {
        (None, None, None) => (32, 3),
        (Some(max_concurrent_requests), None, None) => {
            (std::cmp::max(max_concurrent_requests / 3, 1), 3)
        }
        (None, Some(max_concurrent_chunks), None) => (max_concurrent_chunks, 3),
        (None, None, Some(max_concurrent_blocks)) => (
            std::cmp::max(100 / max_concurrent_blocks, 1),
            max_concurrent_blocks,
        ),
        (Some(max_concurrent_requests), Some(max_concurrent_chunks), None) => (
            max_concurrent_chunks,
            std::cmp::max(max_concurrent_requests / max_concurrent_chunks, 1),
        ),
        (None, Some(max_concurrent_chunks), Some(max_concurrent_blocks)) => {
            (max_concurrent_chunks, max_concurrent_blocks)
        }
        (Some(max_concurrent_requests), None, Some(max_concurrent_blocks)) => (
            std::cmp::max(max_concurrent_requests / max_concurrent_blocks, 1),
            max_concurrent_blocks,
        ),
        (
            Some(max_concurrent_requests),
            Some(max_concurrent_chunks),
            Some(max_concurrent_blocks),
        ) => {
            if max_concurrent_requests != max_concurrent_chunks * max_concurrent_blocks {
                return Err(eyre::eyre!("max_concurrent_requests should equal max_concurrent_chunks * max_concurrent_blocks"));
            }
            (max_concurrent_chunks, max_concurrent_blocks)
        }
    };
    Ok(result)
}

/// parse block numbers to freeze
pub async fn parse_block_inputs(
    inputs: &Vec<String>,
    provider: &Provider<Http>,
) -> Result<Vec<BlockChunk>> {
    match inputs.len() {
        1 => {
            let first_input = inputs
                .get(0)
                .ok_or_else(|| eyre::eyre!("Failed to get the first input"))?;
            parse_block_token(first_input, true, provider)
                .await
                .map(|x| vec![x])
        }
        _ => {
            let mut chunks = Vec::new();
            for input in inputs {
                chunks.push(parse_block_token(input, false, provider).await?);
            }
            Ok(chunks)
        }
    }
}

enum RangePosition {
    First,
    Last,
    None,
}

async fn parse_block_token(
    s: &str,
    as_range: bool,
    provider: &Provider<Http>,
) -> Result<BlockChunk> {
    let s = s.replace('_', "");
    let parts: Vec<&str> = s.split(':').collect();
    match parts.as_slice() {
        [block_ref] => {
            let block = parse_block_number(block_ref, RangePosition::None, provider).await?;
            Ok(BlockChunk::Numbers(vec![block]))
        }
        [first_ref, second_ref] => {
            let (start_block, end_block) = match (first_ref, second_ref) {
                _ if first_ref.starts_with('-') => {
                    let end_block =
                        parse_block_number(second_ref, RangePosition::Last, provider).await?;
                    let start_block = end_block
                        .checked_sub(first_ref[1..].parse::<u64>()?)
                        .ok_or_else(|| eyre::eyre!("start_block underflow"))?;
                    (start_block, end_block)
                }
                _ if second_ref.starts_with('+') => {
                    let start_block =
                        parse_block_number(first_ref, RangePosition::First, provider).await?;
                    let end_block = start_block
                        .checked_add(second_ref[1..].parse::<u64>()?)
                        .ok_or_else(|| eyre::eyre!("end_block overflow"))?;
                    (start_block, end_block)
                }
                _ => {
                    let start_block =
                        parse_block_number(first_ref, RangePosition::First, provider).await?;
                    let end_block =
                        parse_block_number(second_ref, RangePosition::Last, provider).await?;
                    (start_block, end_block)
                }
            };

            if end_block <= start_block {
                Err(eyre::eyre!("end_block should not be less than start_block"))
            } else if as_range {
                Ok(BlockChunk::Range(start_block, end_block))
            } else {
                Ok(BlockChunk::Numbers((start_block..=end_block).collect()))
            }
        }
        _ => Err(eyre::eyre!(
            "blocks must be in format block_number or start_block:end_block"
        )),
    }
}

async fn parse_block_number(
    block_ref: &str,
    range_position: RangePosition,
    provider: &Provider<Http>,
) -> Result<u64> {
    match (block_ref, range_position) {
        ("latest", _) => provider
            .get_block_number()
            .await
            .map(|n| n.as_u64())
            .wrap_err("Error retrieving latest block number"),
        ("", RangePosition::First) => Ok(0),
        ("", RangePosition::Last) => provider
            .get_block_number()
            .await
            .map(|n| n.as_u64())
            .wrap_err("Error retrieving last block number"),
        ("", RangePosition::None) => Err(eyre::eyre!("invalid input")),
        _ if block_ref.ends_with('B') | block_ref.ends_with('b') => {
            let s = &block_ref[..block_ref.len() - 1];
            s.parse::<f64>()
                .map(|n| (1e9 * n) as u64)
                .wrap_err_with(|| format!("Error parsing block ref '{}'", s))
        }
        _ if block_ref.ends_with('M') | block_ref.ends_with('m') => {
            let s = &block_ref[..block_ref.len() - 1];
            s.parse::<f64>()
                .map(|n| (1e6 * n) as u64)
                .wrap_err_with(|| format!("Error parsing block ref '{}'", s))
        }
        _ if block_ref.ends_with('K') | block_ref.ends_with('k') => {
            let s = &block_ref[..block_ref.len() - 1];
            s.parse::<f64>()
                .map(|n| (1e3 * n) as u64)
                .wrap_err_with(|| format!("Error parsing block ref '{}'", s))
        }
        _ => block_ref
            .parse::<u64>()
            .wrap_err_with(|| format!("Error parsing block ref '{}'", block_ref)),
    }
}
