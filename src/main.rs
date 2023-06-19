mod dataframes;
mod freeze;
mod gather;

use clap::Parser;
use ethers::prelude::*;

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
    #[arg(short, long, default_value = "17000000:17000100")]
    blocks: String,

    /// RPC URL
    #[arg(short, long, default_value = "http://34.105.67.70:8545")]
    // #[arg(short, long, default_value = "ws://34.105.67.70")]
    rpc: String,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let opts = parse_opts();
    freeze::freeze(opts).await;
    Ok(())
}

/// parse options for running freeze
fn parse_opts() -> freeze::FreezeOpts {
    let args = Args::parse();

    // get block range
    let provider = Provider::<Http>::try_from(args.rpc).unwrap();
    let (start_block, end_block, block_chunk_size) = parse_block_str(&args.blocks).unwrap();
    freeze::FreezeOpts {
        datatype: args.datatype,
        provider: provider,
        start_block: Some(start_block),
        end_block: Some(end_block),
        block_chunk_size: block_chunk_size,
        max_concurrent_requests: Some(args.max_concurrent_requests),
    }
}

#[derive(Debug)]
enum BlockParseError {
    InvalidInput(String),
    // ParseError(std::num::ParseIntError),
}

/// parse block numbers to freeze
fn parse_block_str(s: &str) -> Result<(u64, u64, Option<u64>), BlockParseError> {
    let parts: Vec<&str> = s.split(':').collect();
    if parts.len() < 2 || parts.len() > 3 {
        return Err(BlockParseError::InvalidInput(
            "blocks must be in format start_block:end_block or start_block:end_block:chunk_size"
                .to_string(),
        ));
    }
    let first = parts
        .get(0)
        .ok_or("Missing first number")
        .unwrap()
        .parse::<u64>()
        .unwrap();
    let second = parts
        .get(1)
        .ok_or("Missing second number")
        .unwrap()
        .parse::<u64>()
        .unwrap();

    let third = match parts.get(2) {
        Some(t) => Some(t.parse::<u64>().unwrap()),
        None => None,
    };

    Ok((first, second, third))
}
