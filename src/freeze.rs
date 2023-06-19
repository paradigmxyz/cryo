use crate::dataframes;
use crate::gather;

use ethers::prelude::*;
use polars::prelude::*;

pub async fn freeze_blocks_transactions(
    start_block: u64,
    end_block: u64,
    provider: Provider<Http>,
    max_concurrent_requests: usize,
) {
    // get txs
    let block_numbers: Vec<u64> = (start_block..=end_block).collect();
    let txs = gather::get_blocks_txs(block_numbers, provider, max_concurrent_requests).await;

    // create dataframe
    let df: &mut DataFrame = &mut dataframes::txs_to_df(txs.unwrap()).unwrap();

    // print dataframe
    println!("{:?}", df);
    println!("{:?}", df.schema());

    // write to file
    let filename = "txs.parquet";
    dataframes::df_to_parquet(df, &filename);
}
