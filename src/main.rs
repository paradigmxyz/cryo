mod block_utils;
mod dataframes;
mod freeze;
mod gather;
mod types;

use clap::Parser;
use ethers::prelude::*;
use crate::types::FreezeOpts;

/// Command line arguments
#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    #[arg(required = true)]
    datatype: String,

    /// Name of the person to greet
    #[arg(short, long, default_value_t = 200)]
    max_concurrent_requests: usize,

    /// Number of times to greet
    #[arg(short, long, default_value = "17000000:17000100", num_args(0..))]
    blocks: Vec<String>,

    /// Chunk size (blocks per chunk)
    #[arg(short, long, default_value_t = 1000)]
    chunk_size: u64,

    /// RPC URL
    #[arg(short, long, default_value = "http://34.105.67.70:8545")]
    // #[arg(short, long, default_value = "ws://34.105.67.70")]
    rpc: String,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let opts = parse_opts().await;
    freeze::freeze(opts).await;
    Ok(())
}

/// parse options for running freeze
async fn parse_opts() -> FreezeOpts {
    let args = Args::parse();

    // get block range
    let provider = Provider::<Http>::try_from(args.rpc).unwrap();
    let network_name = match provider.get_chainid().await {
        Ok(chain_id) => {
            match chain_id.as_u64() {
                1 => "ethereum".to_string(),
                chain_id => "network_".to_string() + chain_id.to_string().as_str(),
            }
        },
        _ => panic!("could not determine chain_id"),
    };
    let (start_block, end_block, block_numbers) = block_utils::parse_block_inputs(&args.blocks).unwrap();
    FreezeOpts {
        datatype: args.datatype,
        provider: provider,
        max_concurrent_requests: Some(args.max_concurrent_requests),
        start_block: start_block,
        end_block: end_block,
        block_numbers: block_numbers,
        chunk_size: args.chunk_size,
        network_name: network_name,
    }
}

