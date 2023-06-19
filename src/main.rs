// Questions
// - do connections get re-used in concurrent calls
// - is there a way to eliminate copying? (e.g. to_vec())
// - are most ethers functions async?
// Next goals
// - call from python, return result to python
// - upload to s3 instead of saving to disk

mod gather;
mod freeze;

use clap::Parser;
use ethers::prelude::*;
use polars::prelude::*;

/// Command line arguments
#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// Name of the person to greet
    #[arg(short, long, default_value_t = 200)]
    max_concurrent_requests: usize,

    /// Number of times to greet
    #[arg(short, long, default_value_t = 100)]
    n_blocks: u64,

    /// RPC URL
    #[arg(short, long, default_value = "http://34.105.67.70:8545")]
    rpc: String,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();

    let provider = Provider::<Http>::try_from(args.rpc)?;

    // get current block number
    let block_number: U64 = provider.get_block_number().await?;
    let start = block_number.as_u64().saturating_sub(args.n_blocks - 1);
    let block_numbers_to_fetch: Vec<_> = (start..=block_number.as_u64()).collect();

    // get txs
    let txs = gather::get_blocks_txs(
        block_numbers_to_fetch,
        provider,
        args.max_concurrent_requests,
    )
    .await;

    // create dataframe
    let df: &mut DataFrame = &mut freeze::txs_to_df(txs.unwrap()).unwrap();

    // print dataframe
    println!("{:?}", df);
    println!("{:?}", df.schema());

    // write to file
    let filename = "parquet_writer_test.parquet";
    freeze::df_to_parquet(df, &filename);

    Ok(())
}
