mod block_utils;
mod dataframes;
mod datatype_utils;
mod freeze;
mod gather;
mod types;

use crate::types::{ColumnEncoding, Datatype, FileFormat, FreezeOpts, Schema};
use clap::Parser;
use ethers::prelude::*;
use std::collections::HashMap;
use std::fs;

use anstyle;

/// Command line arguments
#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None, styles=get_styles())]
struct Args {
    /// datatype(s) to collect, see above
    #[arg(required = true, num_args(1..))]
    datatype: Vec<String>,

    /// Block numbers, either individually or start:end ranges
    #[arg(short, long, default_value = "17000000:17000100", num_args(0..))]
    blocks: Vec<String>,

    /// RPC URL
    #[arg(short, long, default_value = "http://34.105.67.70:8545")]
    rpc: String,

    /// Network name, by default will derive from eth_getChainId
    #[arg(long)]
    network_name: Option<String>,

    /// Chunk size (blocks per chunk)
    #[arg(short, long, default_value_t = 1000)]
    chunk_size: u64,

    /// Directory for output files
    #[arg(short, long, default_value = ".")]
    output_dir: String,

    /// Save as csv instead of parquet
    #[arg(long)]
    csv: bool,

    /// Use hex string encoding for columns of binary data
    #[arg(long)]
    hex: bool,

    /// Columns to include in output
    #[arg(short, long, num_args(0..))]
    include_columns: Option<Vec<String>>,

    /// Columns to exclude from output
    #[arg(short, long, num_args(0..))]
    exclude_columns: Option<Vec<String>>,

    /// Global number of concurrent requests
    #[arg(long)]
    max_concurrent_requests: Option<u64>,

    /// Number of chunks processed concurrently
    #[arg(long)]
    max_concurrent_chunks: Option<u64>,

    /// Number blocks within a chunk processed concurrently
    #[arg(long)]
    max_concurrent_blocks: Option<u64>,

    /// Number of blocks per log request
    #[arg(long, default_value_t = 1)]
    log_request_size: u64,

    /// Dry run
    #[arg(short, long)]
    dry: bool,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let (opts, args) = parse_opts().await;
    print_cryo_summary(&opts, &args);
    if opts.dry_run {
        println!("[dry run, exiting]");
    } else {
        println!("");
        println!("collecting data...");
        freeze::freeze(opts).await?;
        println!("...done");
    };
    Ok(())
}

pub fn get_styles() -> clap::builder::Styles {
    let white = anstyle::Color::Rgb(anstyle::RgbColor(255, 255, 255));
    let green = anstyle::Color::Rgb(anstyle::RgbColor(0, 225, 0));
    let grey = anstyle::Color::Rgb(anstyle::RgbColor(170, 170, 170));
    let title = anstyle::Style::new()
        .bold()
        .underline()
        .fg_color(Some(green));
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


fn parse_datatype(datatype: &str) -> Datatype {
    match datatype {
        "blocks" => Datatype::Blocks,
        "logs" => Datatype::Logs,
        "transactions" => Datatype::Transactions,
        _ => panic!("invalid datatype"),
    }
}

/// parse options for running freeze
async fn parse_opts() -> (FreezeOpts, Args) {
    // parse args
    let args = Args::parse();

    let datatypes: Vec<Datatype> = args
        .datatype
        .iter()
        .map(|datatype| parse_datatype(datatype))
        .collect();

    // parse block chunks
    let block_chunk = block_utils::parse_block_inputs(&args.blocks).unwrap();
    let block_chunks = block_utils::get_subchunks(&block_chunk, &args.chunk_size);

    // parse network info
    let provider = Provider::<Http>::try_from(args.rpc.clone()).unwrap();
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

    // process output directory
    let output_dir = args.output_dir.clone();
    match fs::create_dir_all(&output_dir) {
        Ok(_) => {}
        Err(e) => panic!("Error creating directory: {}", e),
    };

    // process output formats
    let output_format = match args.csv {
        true => FileFormat::Csv,
        false => FileFormat::Parquet,
    };
    let binary_column_format = match args.hex {
        true => ColumnEncoding::Hex,
        false => ColumnEncoding::Binary,
    };

    // process concurrency info
    let (max_concurrent_chunks, max_concurrent_blocks) = parse_concurrency_args(&args);

    // process schemas
    let schemas: HashMap<Datatype, Schema> = HashMap::from_iter(datatypes.iter().map(|datatype| {
        let schema: Schema = datatype_utils::get_schema(
            &datatype,
            &binary_column_format,
            &args.include_columns,
            &args.exclude_columns,
        );
        (datatype.clone(), schema)
    }));

    // compile opts
    let opts = FreezeOpts {
        datatypes: datatypes,
        provider: provider,
        block_chunks: block_chunks,
        output_dir: output_dir,
        output_format: output_format,
        binary_column_format: binary_column_format,
        network_name: network_name.clone(),
        max_concurrent_chunks: max_concurrent_chunks,
        max_concurrent_blocks: max_concurrent_blocks,
        log_request_size: args.log_request_size,
        dry_run: args.dry,
        schemas: schemas,
    };

    (opts, args)
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

fn print_cryo_summary(opts: &FreezeOpts, args: &Args) {
    println!("cryo parameters:");
    let datatype_strs: Vec<_> = opts.datatypes.iter().map(|d| d.as_str()).collect();
    println!("- datatypes: {}", datatype_strs.join(", "));
    println!("- network: {}", opts.network_name);
    println!("- provider: {}", args.rpc);
    println!(
        "- total blocks: {}",
        block_utils::get_total_blocks(&opts.block_chunks)
    );
    println!("- block chunk size: {}", args.chunk_size);
    println!("- total block chunks: {}", opts.block_chunks.len());
    println!("- max concurrent chunks: {}", opts.max_concurrent_chunks);
    println!("- max concurrent blocks: {}", opts.max_concurrent_blocks);
    if opts.datatypes.contains(&Datatype::Logs) {
        println!("- log request size: {}", opts.log_request_size);
    };
    println!("- output format: {}", opts.output_format.as_str());
    println!(
        "- binary column format: {}",
        opts.binary_column_format.as_str()
    );
    println!("- output dir: {}", opts.output_dir);
    print_schemas(&opts.schemas);
}

fn print_schemas(schemas: &HashMap<Datatype, Schema>) {
    schemas.iter().for_each(|(name, schema)| {
        println!("");
        print_schema(&name, &schema)
    })
}

fn print_schema(name: &Datatype, schema: &Schema) {
    println!("schema for {}:", name.as_str());
    schema.iter().for_each(|(name, column_type)| {
        println!("- {}: {}", name, column_type.as_str());
    })
}
