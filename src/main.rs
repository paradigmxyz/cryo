mod block_utils;
mod dataframes;
mod freeze;
mod gather;
mod types;

use crate::types::FreezeOpts;
use clap::Parser;
use ethers::prelude::*;

/// Command line arguments
#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    #[arg(required = true)]
    datatype: String,

    /// Number of times to greet
    #[arg(short, long, default_value = "17000000:17000100", num_args(0..))]
    blocks: Vec<String>,

    /// RPC URL
    #[arg(short, long, default_value = "http://34.105.67.70:8545")]
    rpc: String,

    /// Chunk size (blocks per chunk)
    #[arg(short, long, default_value_t = 1000)]
    chunk_size: u64,

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
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let (opts, args) = parse_opts().await;
    print_cryo_summary(&opts, &args);
    freeze::freeze(opts).await?;
    Ok(())
}

/// parse options for running freeze
async fn parse_opts() -> (FreezeOpts, Args) {
    // parse args
    let args = Args::parse();

    // parse block chunks
    let block_chunk = block_utils::parse_block_inputs(&args.blocks).unwrap();
    let block_chunks = block_utils::get_subchunks(&block_chunk, &args.chunk_size);

    // parse network info
    let provider = Provider::<Http>::try_from(args.rpc.clone()).unwrap();
    let network_name = match provider.get_chainid().await {
        Ok(chain_id) => match chain_id.as_u64() {
            1 => "ethereum".to_string(),
            chain_id => "network_".to_string() + chain_id.to_string().as_str(),
        },
        _ => panic!("could not determine chain_id"),
    };

    // parse concurrency info
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
        (None, None, Some(max_concurrent_blocks)) => {
            (std::cmp::max(100 / max_concurrent_blocks, 1), max_concurrent_blocks)
        }
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
    };
    let (max_concurrent_chunks, max_concurrent_blocks) = result;

    let opts = FreezeOpts {
        datatype: args.datatype.clone(),
        provider: provider,
        block_chunks: block_chunks,
        network_name: network_name,
        max_concurrent_chunks: max_concurrent_chunks,
        max_concurrent_blocks: max_concurrent_blocks,
        log_request_size: args.log_request_size,
    };

    (opts, args)
}

fn print_cryo_summary(opts: &FreezeOpts, args: &Args) {
    println!("performing cryo freeze...");
    println!("- datatype: {}", opts.datatype);
    println!("- network: {}", opts.network_name);
    println!("- provider: {}", args.rpc);
    println!("- n_block_chunks: {}", opts.block_chunks.len());
    println!("- max_concurrent_chunks: {}", opts.max_concurrent_chunks);
    println!("- max_concurrent_blocks: {}", opts.max_concurrent_blocks);
    if opts.datatype == "logs" {
        println!("- log_request_size: {}", opts.log_request_size);
    }
}

