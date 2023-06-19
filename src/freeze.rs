use crate::block_utils;
use crate::dataframes;
use crate::gather;
use crate::types::{BlockChunk, FreezeOpts, SlimBlock};

use ethers::prelude::*;
use polars::prelude::*;

pub async fn freeze(opts: FreezeOpts) {
    let chunks = block_utils::get_chunks(&opts);
    for chunk in chunks {
        match &opts.datatype[..] {
            "blocks_and_transactions" => freeze_blocks_and_transactions_chunk(chunk, &opts).await,
            "blocks" => freeze_blocks_chunk(chunk, &opts).await,
            "transactions" => freeze_transactions_chunk(chunk, &opts).await,
            _ => println!("invalid datatype"),
        }
    }
}

fn get_file_path(name: String, _opts: &FreezeOpts) -> String {
    name + ".parquet"
}

async fn freeze_blocks_and_transactions_chunk(chunk: BlockChunk, opts: &FreezeOpts) {
    let block_numbers = block_utils::get_chunk_block_numbers(chunk);
    let (blocks, txs) = gather::get_blocks_and_transactions(block_numbers, &opts)
        .await
        .unwrap();
    save_blocks(blocks, get_file_path("blocks".to_string(), &opts));
    save_transactions(txs, get_file_path("transactions".to_string(), &opts));
}

async fn freeze_blocks_chunk(chunk: BlockChunk, opts: &FreezeOpts) {
    let block_numbers = block_utils::get_chunk_block_numbers(chunk);
    let blocks = gather::get_blocks(block_numbers, &opts).await.unwrap();
    save_blocks(blocks, get_file_path("blocks".to_string(), &opts));
}

async fn freeze_transactions_chunk(chunk: BlockChunk, opts: &FreezeOpts) {
    let block_numbers = block_utils::get_chunk_block_numbers(chunk);
    let txs = gather::get_transactions(block_numbers, &opts)
        .await
        .unwrap();
    save_transactions(txs, get_file_path("transactions".to_string(), &opts));
}

fn save_blocks(blocks: Vec<SlimBlock>, path: String) {
    let df_blocks: &mut DataFrame = &mut dataframes::blocks_to_df(blocks).unwrap();
    dataframes::preview_df(df_blocks, "blocks");
    dataframes::df_to_parquet(df_blocks, &path);
}

fn save_transactions(txs: Vec<Transaction>, path: String) {
    let df_txs: &mut DataFrame = &mut dataframes::txs_to_df(txs).unwrap();
    dataframes::preview_df(df_txs, "transactions");
    dataframes::df_to_parquet(df_txs, &path);
}
