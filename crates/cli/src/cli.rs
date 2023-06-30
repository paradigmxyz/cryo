use std::collections::HashMap;
use std::env;
use std::fs;

use clap::Parser;
use color_print::cstr;
use ethers::prelude::*;
use hex::FromHex;
use polars::prelude::*;

use cryo_freezer::BlockChunk;
use cryo_freezer::ColumnEncoding;
use cryo_freezer::CompressionParseError;
use cryo_freezer::Datatype;
use cryo_freezer::FileFormat;
use cryo_freezer::FreezeOpts;
use cryo_freezer::Schema;

/// Command line arguments
#[derive(Parser, Debug)]
#[command(name = "cryo", author, version, about = get_about_str(), long_about = None, styles=get_styles(), after_help=get_after_str())]
pub struct Args {
    #[arg(required = true, help=get_datatype_help(), num_args(1..))]
    datatype: Vec<String>,

    /// Block numbers, see syntax above
    #[arg(
        short,
        long,
        default_value = "0:latest",
        allow_hyphen_values(true),
        help_heading = "Content Options"
    )]
    blocks: Vec<String>,

    /// Number of blocks per chunk
    #[arg(short, long, default_value_t = 1000, help_heading = "Output Options")]
    pub chunk_size: u64,

    /// Number of chunks (alternative to --chunk-size)
    #[arg(long, help_heading = "Output Options")]
    pub n_chunks: Option<u64>,

    #[arg(
        long,
        default_value = "20min",
        help_heading = "Content Options",
        help = "Reorg buffer, save blocks only when they are this old\ncan be a number of blocks or a time"
    )]
    reorg_buffer: String,

    /// Columns to include in output
    #[arg(short, long, value_name="COLS", num_args(0..), help_heading="Content Options")]
    include_columns: Option<Vec<String>>,

    /// Columns to exclude from output
    #[arg(short, long, value_name="COLS", num_args(0..), help_heading="Content Options")]
    exclude_columns: Option<Vec<String>>,

    /// RPC url [default: ETH_RPC_URL env var]
    #[arg(short, long, help_heading = "Source Options")]
    pub rpc: Option<String>,

    /// Network name [default: use name of eth_getChainId]
    #[arg(long, help_heading = "Source Options")]
    network_name: Option<String>,

    /// Ratelimit on requests per second
    #[arg(
        short('l'),
        long,
        value_name = "limit",
        help_heading = "Acquisition Options"
    )]
    requests_per_second: Option<u64>,

    /// Global number of concurrent requests
    #[arg(long, value_name = "M", help_heading = "Acquisition Options")]
    max_concurrent_requests: Option<u64>,

    /// Number of chunks processed concurrently
    #[arg(long, value_name = "M", help_heading = "Acquisition Options")]
    max_concurrent_chunks: Option<u64>,

    /// Number blocks within a chunk processed concurrently
    #[arg(long, value_name = "M", help_heading = "Acquisition Options")]
    max_concurrent_blocks: Option<u64>,

    /// Dry run, collect no data
    #[arg(short, long, help_heading = "Acquisition Options")]
    dry: bool,

    /// Directory for output files
    #[arg(short, long, default_value = ".", help_heading = "Output Options")]
    output_dir: String,

    /// Overwrite existing files instead of skipping them
    #[arg(long, help_heading = "Output Options")]
    overwrite: bool,

    /// Save as csv instead of parquet
    #[arg(long, help_heading = "Output Options")]
    csv: bool,

    /// Save as json instead of parquet
    #[arg(long, help_heading = "Output Options")]
    json: bool,

    /// Use hex string encoding for binary columns
    #[arg(long, help_heading = "Output Options")]
    hex: bool,

    /// Columns(s) to sort by
    #[arg(short, long, num_args(0..), help_heading="Output Options")]
    sort: Vec<String>,

    /// Number of rows per row group in parquet file
    #[arg(long, value_name = "GROUP_SIZE", help_heading = "Output Options")]
    row_group_size: Option<usize>,

    /// Number of rows groups in parquet file
    #[arg(long, help_heading = "Output Options")]
    n_row_groups: Option<usize>,

    /// Do not write statistics to parquet files
    #[arg(long, help_heading = "Output Options")]
    no_stats: bool,

    /// Set compression algorithm and level
    #[arg(long, help_heading="Output Options", value_name="NAME [#]", num_args(1..=2), default_value = "lz4")]
    compression: Vec<String>,

    /// [transactions] track gas used by each transaction
    #[arg(long, help_heading = "Dataset-specific Options")]
    gas_used: bool,

    /// [logs] filter logs by contract address
    #[arg(long, help_heading = "Dataset-specific Options")]
    contract: Option<String>,

    /// [logs] filter logs by topic0
    #[arg(
        long,
        visible_alias = "event",
        help_heading = "Dataset-specific Options"
    )]
    topic0: Option<String>,

    /// [logs] filter logs by topic1
    #[arg(long, help_heading = "Dataset-specific Options")]
    topic1: Option<String>,

    /// [logs] filter logs by topic2
    #[arg(long, help_heading = "Dataset-specific Options")]
    topic2: Option<String>,

    /// [logs] filter logs by topic3
    #[arg(long, help_heading = "Dataset-specific Options")]
    topic3: Option<String>,

    /// [logs] Number of blocks per log request
    #[arg(
        long,
        value_name = "N_BLOCKS",
        default_value_t = 1,
        help_heading = "Dataset-specific Options"
    )]
    log_request_size: u64,
}

pub fn get_styles() -> clap::builder::Styles {
    let white = anstyle::Color::Rgb(anstyle::RgbColor(255, 255, 255));
    let green = anstyle::Color::Rgb(anstyle::RgbColor(0, 225, 0));
    let grey = anstyle::Color::Rgb(anstyle::RgbColor(170, 170, 170));
    let title = anstyle::Style::new().bold().fg_color(Some(green));
    let arg = anstyle::Style::new().bold().fg_color(Some(white));
    let comment = anstyle::Style::new().fg_color(Some(grey));
    clap::builder::Styles::styled()
        .header(title)
        .error(comment)
        .usage(title)
        .literal(arg)
        .placeholder(comment)
        .valid(title)
        .invalid(comment)
}

fn get_about_str() -> &'static str {
    cstr!(r#"<white><bold>cryo</bold></white> extracts blockchain data to parquet, csv, or json"#)
}

fn get_after_str() -> &'static str {
    cstr!(
        r#"
<white><bold>Block specification syntax</bold></white>
- can use numbers                    <white><bold>--blocks 5000 6000 7000</bold></white>
- can use ranges                     <white><bold>--blocks 12M:13M 15M:16M</bold></white>
- numbers can contain { _ . K M B }  <white><bold>5_000 5K 15M 15.5M</bold></white>
- omiting range end means latest     <white><bold>15.5M:</bold></white> == <white><bold>15.5M:latest</bold></white>
- omitting range start means 0       <white><bold>:700</bold></white> == <white><bold>0:700</bold></white>
- minus on start means minus end     <white><bold>-1000:7000</bold></white> == <white><bold>6000:7000</bold></white>
- plus sign on end means plus start  <white><bold>15M:+1000</bold></white> == <white><bold>15M:15.001K</bold></white>
"#
    )
}

fn get_datatype_help() -> &'static str {
    cstr!(
        r#"datatype(s) to collect, one or more of:
- <white><bold>blocks</bold></white>
- <white><bold>logs</bold></white>
- <white><bold>transactions</bold></white>
- <white><bold>call_traces</bold></white>
- <white><bold>state_diffs</bold></white>
- <white><bold>balance_diffs</bold></white>
- <white><bold>code_diffs</bold></white>
- <white><bold>slot_diffs</bold></white>
- <white><bold>nonce_diffs</bold></white>
- <white><bold>opcode_traces</bold></white>"#
    )
}

/// parse options for running freeze
pub async fn parse_opts() -> FreezeOpts {
    // parse args
    let args = Args::parse();

    let datatypes: Vec<Datatype> = args
        .datatype
        .iter()
        .map(|datatype| parse_datatype(datatype))
        .collect();

    // parse network info
    let rpc_url = parse_rpc_url(&args);
    let provider = Provider::<Http>::try_from(rpc_url).unwrap();
    let network_name = match &args.network_name {
        Some(name) => name.clone(),
        None => match provider.get_chainid().await {
            Ok(chain_id) => match chain_id.as_u64() {
                1 => "ethereum".to_string(),
                chain_id => "network_".to_string() + chain_id.to_string().as_str(),
            },
            _ => panic!("could not determine chain_id"),
        },
    };

    // parse block chunks
    let block_chunk = parse_block_inputs(&args.blocks, &provider).await.unwrap();
    let block_chunks = match args.n_chunks {
        Some(n_chunks) => cryo_freezer::get_subchunks_by_count(&block_chunk, &n_chunks),
        None => cryo_freezer::get_subchunks_by_size(&block_chunk, &args.chunk_size),
    };
    if args.reorg_buffer != "0" {
        println!("reorg")
    }

    // process output directory
    let output_dir = std::fs::canonicalize(args.output_dir.clone())
        .unwrap()
        .to_string_lossy()
        .into_owned();
    match fs::create_dir_all(&output_dir) {
        Ok(_) => {}
        Err(e) => panic!("Error creating directory: {}", e),
    };

    // process output formats
    let output_format = match (args.csv, args.json) {
        (true, true) => panic!("choose one of parquet, csv, or json"),
        (true, _) => FileFormat::Csv,
        (_, true) => FileFormat::Json,
        (false, false) => FileFormat::Parquet,
    };
    if output_format != FileFormat::Parquet {
        panic!("non-parquet not supported until hex encoding implemented");
    };
    let binary_column_format = match args.hex {
        true => ColumnEncoding::Hex,
        false => ColumnEncoding::Binary,
    };

    // process concurrency info
    let (max_concurrent_chunks, max_concurrent_blocks) = parse_concurrency_args(&args);

    // process schemas
    let schemas: HashMap<Datatype, Schema> = HashMap::from_iter(datatypes.iter().map(|datatype| {
        let schema: Schema = datatype.get_schema(
            &binary_column_format,
            &args.include_columns,
            &args.exclude_columns,
        ).unwrap();
        (*datatype, schema)
    }));

    let sort = parse_sort(&args.sort, &schemas);

    let contract = parse_address(&args.contract);
    let topic0 = parse_topic(&args.topic0);
    let topic1 = parse_topic(&args.topic1);
    let topic2 = parse_topic(&args.topic2);
    let topic3 = parse_topic(&args.topic3);

    let parquet_compression = parse_compression(&args.compression).unwrap();

    let row_group_size = parse_row_group_size(args.row_group_size, args.n_row_groups);

    // compile opts
    FreezeOpts {
        datatypes,
        // content options
        block_chunks,
        schemas,
        // source options
        provider,
        network_name,
        // acquisition options
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
        gas_used: args.gas_used,
        contract,
        topic0,
        topic1,
        topic2,
        topic3,
        log_request_size: args.log_request_size,
    }
}

fn parse_datatype(datatype: &str) -> Datatype {
    match datatype {
        "blocks" => Datatype::Blocks,
        "logs" => Datatype::Logs,
        "events" => Datatype::Logs,
        "transactions" => Datatype::Transactions,
        "txs" => Datatype::Transactions,
        _ => panic!("{}", ("invalid datatype ".to_string() + datatype)),
    }
}

pub fn parse_row_group_size(
    row_group_size: Option<usize>,
    n_row_groups: Option<usize>,
) -> Option<usize> {
    match (row_group_size, n_row_groups) {
        (Some(row_group_size), _) => Some(row_group_size),
        (_, Some(n_row_groups)) => Some(n_row_groups),
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

fn parse_compression(input: &Vec<String>) -> Result<ParquetCompression, CompressionParseError> {
    match input.as_slice() {
        [algorithm] if algorithm.as_str() == "uncompressed" => Ok(ParquetCompression::Uncompressed),
        [algorithm] if algorithm.as_str() == "snappy" => Ok(ParquetCompression::Snappy),
        [algorithm] if algorithm.as_str() == "lzo" => Ok(ParquetCompression::Lzo),
        [algorithm] if algorithm.as_str() == "lz4" => Ok(ParquetCompression::Lz4Raw),
        [algorithm, level_str] if algorithm.as_str() == "gzip" => match level_str.parse::<u8>() {
            Ok(level) => match GzipLevel::try_new(level) {
                Ok(gzip_level) => Ok(ParquetCompression::Gzip(Some(gzip_level))),
                Err(_) => Err(CompressionParseError::InvalidCompressionLevel),
            },
            Err(_) => Err(CompressionParseError::InvalidCompressionLevel),
        },
        [algorithm, level_str] if algorithm.as_str() == "brotli" => {
            match level_str.parse::<u32>() {
                Ok(level) => match BrotliLevel::try_new(level) {
                    Ok(brotli_level) => Ok(ParquetCompression::Brotli(Some(brotli_level))),
                    Err(_) => Err(CompressionParseError::InvalidCompressionLevel),
                },
                Err(_) => Err(CompressionParseError::InvalidCompressionLevel),
            }
        }
        [algorithm, level_str] if algorithm.as_str() == "zstd" => match level_str.parse::<i32>() {
            Ok(level) => match ZstdLevel::try_new(level) {
                Ok(zstd_level) => Ok(ParquetCompression::Zstd(Some(zstd_level))),
                Err(_) => Err(CompressionParseError::InvalidCompressionLevel),
            },
            Err(_) => Err(CompressionParseError::InvalidCompressionLevel),
        },
        [algorithm] if ["gzip", "brotli", "zstd"].contains(&algorithm.as_str()) => {
            Err(CompressionParseError::MissingCompressionLevel)
        }
        _ => Err(CompressionParseError::InvalidCompressionAlgorithm),
    }
}

fn parse_sort(
    raw_sort: &Vec<String>,
    schemas: &HashMap<Datatype, Schema>,
) -> HashMap<Datatype, Vec<String>> {
    if raw_sort.is_empty() {
        HashMap::from_iter(
            schemas
                .iter()
                .map(|(datatype, _schema)| (*datatype, datatype.dataset().default_sort())),
        )
    } else if schemas.len() > 1 {
        panic!("custom sort not supported for multiple schemas")
    } else {
        let datatype = *schemas.keys().next().unwrap();
        HashMap::from_iter([(datatype, raw_sort.clone())])
    }
}

fn parse_concurrency_args(args: &Args) -> (u64, u64) {
    match (
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
            assert!(
                    max_concurrent_requests == max_concurrent_chunks * max_concurrent_blocks,
                    "max_concurrent_requests should equal max_concurrent_chunks * max_concurrent_blocks"
                    );
            (max_concurrent_chunks, max_concurrent_blocks)
        }
    }
}

#[derive(Debug)]
pub enum BlockParseError {
    InvalidInput(String),
    // ParseError(std::num::ParseIntError),
}

/// parse block numbers to freeze
pub async fn parse_block_inputs(
    inputs: &Vec<String>,
    provider: &Provider<Http>,
) -> Result<BlockChunk, BlockParseError> {
    match inputs.len() {
        1 => parse_block_token(inputs.get(0).unwrap(), true, provider).await,
        _ => {
            let mut block_numbers: Vec<u64> = vec![];
            for input in inputs {
                let subchunk = parse_block_token(input, false, provider).await.unwrap();
                block_numbers.extend(subchunk.block_numbers.unwrap());
            }
            let block_chunk = BlockChunk {
                start_block: None,
                end_block: None,
                block_numbers: Some(block_numbers),
            };
            Ok(block_chunk)
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
) -> Result<BlockChunk, BlockParseError> {
    let s = s.replace('_', "");
    let parts: Vec<&str> = s.split(':').collect();
    match parts.as_slice() {
        [block_ref] => {
            let block = parse_block_number(block_ref, RangePosition::None, provider).await;
            Ok(BlockChunk {
                block_numbers: Some(vec![block]),
                ..Default::default()
            })
        }
        [first_ref, second_ref] => {
            let (start_block, end_block) = match (first_ref, second_ref) {
                _ if first_ref.starts_with('-') => {
                    let end_block =
                        parse_block_number(second_ref, RangePosition::Last, provider).await;
                    let start_block = end_block - first_ref[1..].parse::<u64>().unwrap();
                    (start_block, end_block)
                }
                _ if second_ref.starts_with('+') => {
                    let start_block =
                        parse_block_number(first_ref, RangePosition::First, provider).await;
                    let end_block = start_block + second_ref.parse::<u64>().unwrap();
                    (start_block, end_block)
                }
                _ => {
                    let start_block =
                        parse_block_number(first_ref, RangePosition::First, provider).await;
                    let end_block =
                        parse_block_number(second_ref, RangePosition::Last, provider).await;
                    (start_block, end_block)
                }
            };

            if end_block <= start_block {
                println!("{} > {}", start_block, end_block);
                panic!("end_block should not be less than start_block")
            }
            if as_range {
                Ok(BlockChunk {
                    start_block: Some(start_block),
                    end_block: Some(end_block),
                    block_numbers: None,
                })
            } else {
                Ok(BlockChunk {
                    block_numbers: Some((start_block..=end_block).collect()),
                    ..Default::default()
                })
            }
        }
        _ => Err(BlockParseError::InvalidInput(
            "blocks must be in format block_number or start_block:end_block".to_string(),
        )),
    }
}

async fn parse_block_number(
    block_ref: &str,
    range_position: RangePosition,
    provider: &Provider<Http>,
) -> u64 {
    match (block_ref, range_position) {
        ("latest", _) => provider.get_block_number().await.unwrap().as_u64(),
        ("", RangePosition::First) => 0,
        ("", RangePosition::Last) => provider.get_block_number().await.unwrap().as_u64(),
        ("", RangePosition::None) => panic!("invalid input"),
        _ if block_ref.ends_with('B') | block_ref.ends_with('b') => {
            let s = &block_ref[..block_ref.len() - 1];
            (1e9 * s.parse::<f64>().unwrap()) as u64
        }
        _ if block_ref.ends_with('M') | block_ref.ends_with('m') => {
            let s = &block_ref[..block_ref.len() - 1];
            (1e6 * s.parse::<f64>().unwrap()) as u64
        }
        _ if block_ref.ends_with('K') | block_ref.ends_with('k') => {
            let s = &block_ref[..block_ref.len() - 1];
            (1e3 * s.parse::<f64>().unwrap()) as u64
        }
        _ => block_ref.parse::<u64>().unwrap(),
    }
}
