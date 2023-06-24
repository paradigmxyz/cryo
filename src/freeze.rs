use crate::block_utils;
use crate::dataframes;
use crate::gather;
use crate::types::{BlockChunk, FreezeOpts, SlimBlock};

use tokio::sync::Semaphore;
use std::sync::Arc;

use ethers::prelude::*;
use polars::prelude::*;
use futures::future::try_join_all;
use std::error::Error;


pub async fn freeze(opts: FreezeOpts) -> Result<(), Box<dyn Error>> {
    let sem = Arc::new(Semaphore::new(opts.max_concurrent_chunks as usize));
    let opts = Arc::new(opts);

    let tasks: Vec<_> = opts.block_chunks.clone().into_iter().map(|chunk| {
        let sem = Arc::clone(&sem);
        let opts = Arc::clone(&opts);
        tokio::spawn(async move {
            let _permit = sem.acquire().await.expect("Semaphore acquire");
            match &opts.datatype[..] {
                "blocks_and_transactions" => freeze_blocks_and_transactions_chunk(chunk, &opts).await,
                "blocks" => freeze_blocks_chunk(chunk, &opts).await,
                "transactions" => freeze_transactions_chunk(chunk, &opts).await,
                "logs" => freeze_logs(chunk, &opts).await,
                // "traces" => freeze_traces(chunk, &*opts).await,
                _ => println!("invalid datatype"),
            }
        })
    }).collect();

    let _results = try_join_all(tasks).await?;
    Ok(())
}

async fn freeze_blocks_and_transactions_chunk(chunk: BlockChunk, opts: &FreezeOpts) {
    let block_numbers = block_utils::get_chunk_block_numbers(&chunk);
    let (blocks, txs) = gather::get_blocks_and_transactions(block_numbers, &opts)
        .await
        .unwrap();
    save_blocks(blocks, &chunk, &opts);
    save_transactions(txs, &chunk, &opts);
}

async fn freeze_blocks_chunk(chunk: BlockChunk, opts: &FreezeOpts) {
    let block_numbers = block_utils::get_chunk_block_numbers(&chunk);
    let blocks = gather::get_blocks(block_numbers, &opts).await.unwrap();
    save_blocks(blocks, &chunk, &opts);
}

async fn freeze_transactions_chunk(chunk: BlockChunk, opts: &FreezeOpts) {
    let block_numbers = block_utils::get_chunk_block_numbers(&chunk);
    let txs = gather::get_transactions(block_numbers, &opts)
        .await
        .unwrap();
    save_transactions(txs, &chunk, &opts);
}

async fn freeze_logs(chunk: BlockChunk, opts: &FreezeOpts) {
    let logs = gather::get_logs(&chunk, None, [None, None, None, None], &opts)
        .await
        .unwrap();
    save_logs(logs, &chunk, &opts);
}

// async fn freeze_traces(chunk: BlockChunk, opts: &FreezeOpts) {
//     let logs = gather::freeze_traces(&chunk, None, [None, None, None, None], &opts)
//         .await
//         .unwrap();
//     save_logs(logs, &chunk, &opts);
// }

// saving

fn get_file_path(name: &str, chunk: &BlockChunk, opts: &FreezeOpts) -> String {
    let block_chunk_stub = block_utils::get_block_chunk_stub(chunk);
    opts.network_name.as_str().to_owned() + "__" + name + "__" + block_chunk_stub.as_str() + ".parquet"
}

fn save_blocks(blocks: Vec<SlimBlock>, chunk: &BlockChunk, opts: &FreezeOpts) {
    let path = get_file_path("blocks", chunk, &opts);
    let df: &mut DataFrame = &mut dataframes::blocks_to_df(blocks).unwrap();
    dataframes::df_to_parquet(df, &path);
}

fn save_transactions(txs: Vec<Transaction>, chunk: &BlockChunk, opts: &FreezeOpts) {
    let path = get_file_path("transactions", chunk, &opts);
    let df: &mut DataFrame = &mut dataframes::txs_to_df(txs).unwrap();
    dataframes::df_to_parquet(df, &path);
}

fn save_logs(logs: Vec<Log>, chunk: &BlockChunk, opts: &FreezeOpts) {
    let path = get_file_path("logs", chunk, &opts);
    let df: &mut DataFrame = &mut dataframes::logs_to_df(logs).unwrap();
    dataframes::df_to_parquet(df, &path);
}

