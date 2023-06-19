use crate::dataframes;
use crate::gather;

use ethers::prelude::*;
use polars::prelude::*;

pub struct FreezeOpts {
    pub datatype: String,
    pub provider: Provider<Http>,
    pub max_concurrent_requests: Option<usize>,
    pub start_block: Option<u64>,
    pub end_block: Option<u64>,
    pub block_chunk_size: Option<u64>,
}

pub async fn freeze(opts: FreezeOpts) {
    match &opts.datatype[..] {
        "blocks_and_transactions" => process_blocks_and_transactions(opts).await,
        "blocks" => process_blocks(opts).await,
        "transactions" => process_transactions(opts).await,
        _ => println!("invalid datatype"),
    }
}

fn get_block_numbers(opts: &FreezeOpts) -> Vec<u64> {
    (opts.start_block.unwrap()..=opts.end_block.unwrap()).collect()
}

fn get_file_path(name: String, _opts: &FreezeOpts) -> String {
    name + ".parquet"
}

async fn process_blocks_and_transactions(opts: FreezeOpts) {
    // get data
    let block_numbers = get_block_numbers(&opts);
    let (blocks, txs) =
        gather::get_blocks_and_transactions(block_numbers, &opts.provider, &opts.max_concurrent_requests)
            .await
            .unwrap();

    // freeze data
    freeze_blocks(blocks, get_file_path("blocks".to_string(), &opts));
    freeze_txs(txs, get_file_path("transactions".to_string(), &opts));
}

async fn process_blocks(opts: FreezeOpts) {
    // get data
    let block_numbers = get_block_numbers(&opts);
    let blocks =
        gather::get_blocks(block_numbers, &opts.provider, &opts.max_concurrent_requests)
            .await
            .unwrap();

    // freeze data
    freeze_blocks(blocks, get_file_path("blocks".to_string(), &opts));
}

async fn process_transactions(opts: FreezeOpts) {
    // get data
    let block_numbers = get_block_numbers(&opts);
    let txs =
        gather::get_transactions(block_numbers, &opts.provider, &opts.max_concurrent_requests)
            .await
            .unwrap();

    // freeze data
    freeze_txs(txs, get_file_path("transactions".to_string(), &opts));
}


fn freeze_blocks(blocks: Vec<gather::SlimBlock>, path: String) {
    let df_blocks: &mut DataFrame = &mut dataframes::blocks_to_df(blocks).unwrap();
    dataframes::preview_df(df_blocks, "blocks");
    dataframes::df_to_parquet(df_blocks, &path);
}

fn freeze_txs(txs: Vec<Transaction>, path: String) {
    let df_txs: &mut DataFrame = &mut dataframes::txs_to_df(txs).unwrap();
    dataframes::preview_df(df_txs, "transactions");
    dataframes::df_to_parquet(df_txs, &path);
}
