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
    #[arg(short, long, default_value_t = 100)]
    n_blocks: u64,

    /// RPC URL
    #[arg(short, long, default_value = "http://34.105.67.70:8545")]
    // #[arg(short, long, default_value = "ws://34.105.67.70")]
    rpc: String,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // parse arguments
    let args = Args::parse();

    // get block range
    let provider = Provider::<Http>::try_from(args.rpc)?;
    let end_block = provider.get_block_number().await?.as_u64();
    let start_block = end_block.saturating_sub(args.n_blocks - 1);

    match &args.datatype[..] {
        "transactions" => freeze::freeze_blocks_transactions(
                start_block,
                end_block,
                provider,
                args.max_concurrent_requests
            ).await,
        _ => println!("invalid datatype")
    }

    Ok(())
}

