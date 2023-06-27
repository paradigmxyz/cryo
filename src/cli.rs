use std::collections::HashMap;
use std::env;
use std::fs;

use clap::Parser;
use color_print::cstr;
use ethers::prelude::*;

use crate::chunks;
use crate::types::BlockChunk;
use crate::types::ColumnEncoding;
use crate::types::Datatype;
use crate::types::FileFormat;
use crate::types::FreezeOpts;
use crate::types::Schema;

/// Command line arguments
#[derive(Parser, Debug)]
#[command(author, version, about = get_about_str(), long_about = None, styles=get_styles())]
pub struct Args {
    #[arg(required = true, help=get_datatype_help(), num_args(1..))]
    datatype: Vec<String>,

    /// Block numbers, either numbers or start:end ranges
    #[arg(short, long, default_value = "0:latest", num_args(0..), help_heading="Content Options")]
    blocks: Vec<String>,

    #[arg(
        long,
        default_value = "20min",
        help_heading = "Content Options",
        help = "Reorg buffer, avoid saving any blocks unless at least this old\ncan be a number of blocks or a length of time"
    )]
    reorg_buffer: String,

    /// Columns to include in output
    #[arg(short, long, value_name="COLS", num_args(0..), help_heading="Content Options")]
    include_columns: Option<Vec<String>>,

    /// Columns to exclude from output
    #[arg(short, long, value_name="COLS", num_args(0..), help_heading="Content Options")]
    exclude_columns: Option<Vec<String>>,

    /// RPC URL
    #[arg(short, long, help_heading = "Source Options")]
    pub rpc: Option<String>,

    /// Network name, by default will derive from eth_getChainId
    #[arg(long, help_heading = "Source Options")]
    network_name: Option<String>,

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

    /// Chunk size (blocks per chunk)
    #[arg(short, long, default_value_t = 1000, help_heading = "Output Options")]
    pub chunk_size: u64,

    /// Number of chunks (alternative to --chunk-size)
    #[arg(long, help_heading = "Output Options")]
    pub n_chunks: Option<u64>,

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

    /// Number of rows groups in parquet file
    #[arg(long, help_heading = "Output Options")]
    row_groups: Option<u64>,

    /// Number of rows per row group in parquet file
    #[arg(long, value_name = "GROUP_SIZE", help_heading = "Output Options")]
    row_group_size: Option<u64>,

    /// Do not write statistics to parquet files
    #[arg(long, help_heading = "Output Options")]
    no_stats: bool,

    /// [transactions] get gas used for each transactions
    #[arg(long, help_heading = "Dataset-specific Options")]
    gas_used: bool,

    /// [logs] filter logs by topic1
    #[arg(
        long,
        visible_alias = "event_hash",
        help_heading = "Dataset-specific Options"
    )]
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
pub async fn parse_opts() -> (FreezeOpts, Args) {
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
        Some(n_chunks) => chunks::get_subchunks_by_count(&block_chunk, &n_chunks),
        None => chunks::get_subchunks_by_size(&block_chunk, &args.chunk_size),
    };

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
        );
        (*datatype, schema)
    }));

    let sort = parse_sort(&args.sort, &schemas);

    // compile opts
    let opts = FreezeOpts {
        datatypes,
        provider,
        block_chunks,
        output_dir,
        output_format,
        binary_column_format,
        network_name,
        max_concurrent_chunks,
        max_concurrent_blocks,
        log_request_size: args.log_request_size,
        dry_run: args.dry,
        schemas,
        sort,
        row_groups: args.row_groups,
        row_group_size: args.row_group_size,
        parquet_statistics: !args.no_stats,
        overwrite: args.overwrite,
    };

    (opts, args)
}

fn parse_datatype(datatype: &str) -> Datatype {
    match datatype {
        "blocks" => Datatype::Blocks,
        "logs" => Datatype::Logs,
        "events" => Datatype::Logs,
        "transactions" => Datatype::Transactions,
        "txs" => Datatype::Transactions,
        _ => panic!("invalid datatype"),
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
            let block = parse_block_number(
                block_ref,
                RangePosition::None,
                provider,
            ).await;
            Ok(BlockChunk { block_numbers: Some(vec![block]), ..Default::default() })
        }
        [first_ref, second_ref] => {
            let start_block = parse_block_number(
                first_ref,
                RangePosition::First,
                provider,
            ).await;
            let end_block = parse_block_number(
                second_ref,
                RangePosition::Last,
                provider,
            ).await;
            if end_block <= start_block {
                println!("{} > {}", start_block, end_block);
                panic!("end_block should not be less than start_block")
            }
            if as_range {
                Ok(BlockChunk {
                    start_block: Some(start_block),
                    end_block: Some(end_block),
                    block_numbers: None
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

async fn parse_block_number(block_ref: &str, range_position: RangePosition, provider: &Provider<Http>) -> u64 {
    match (block_ref, range_position) {
        ("latest", _) => provider.get_block_number().await.unwrap().as_u64(),
        ("", RangePosition::First) => 0,
        ("", RangePosition::Last) => provider.get_block_number().await.unwrap().as_u64(),
        ("", RangePosition::None) => panic!("invalid input"),
        _ if block_ref.ends_with('B') | block_ref.ends_with('b') => {
            let s = &block_ref[..block_ref.len()-1];
            (1e9 * s.parse::<f64>().unwrap()) as u64
        }
        _ if block_ref.ends_with('M') | block_ref.ends_with('m') => {
            let s = &block_ref[..block_ref.len()-1];
            (1e6 * s.parse::<f64>().unwrap()) as u64
        }
        _ if block_ref.ends_with('K') | block_ref.ends_with('k') => {
            let s = &block_ref[..block_ref.len()-1];
            (1e3 * s.parse::<f64>().unwrap()) as u64
        }
        _ => block_ref.parse::<u64>().unwrap(),
    }
}
