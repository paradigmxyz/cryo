use crate::block_utils;
use crate::dataframes;
use crate::gather;
use crate::types::{BlockChunk, FreezeOpts, SlimBlock, Datatype};

use std::sync::Arc;
use tokio::sync::Semaphore;

use ethers::prelude::*;
use futures::future::try_join_all;
use polars::prelude::*;
use std::error::Error;

pub async fn freeze(opts: FreezeOpts) -> Result<(), Box<dyn Error>> {
    let sem = Arc::new(Semaphore::new(opts.max_concurrent_chunks as usize));
    let opts = Arc::new(opts);

    let tasks: Vec<_> = opts
        .block_chunks
        .clone()
        .into_iter()
        .map(|chunk| {
            let sem = Arc::clone(&sem);
            let opts = Arc::clone(&opts);
            tokio::spawn(async move {
                let permit = sem.acquire().await.expect("Semaphore acquire");
                freeze_chunk(&chunk, &opts).await;
                drop(permit);
            })
        })
        .collect();

    let _results = try_join_all(tasks).await?;
    Ok(())
}

async fn freeze_chunk(chunk: &BlockChunk, opts: &FreezeOpts) {
    let datatypes = &opts.datatypes;
    if datatypes.contains(&Datatype::Blocks) & datatypes.contains(&Datatype::Transactions) {
        freeze_blocks_and_transactions_chunk(chunk, &opts).await
    } else if datatypes.contains(&Datatype::Blocks) {
        freeze_blocks_chunk(chunk, &opts).await
    } else if datatypes.contains(&Datatype::Transactions) {
        freeze_transactions_chunk(chunk, &opts).await
    }
    if datatypes.contains(&Datatype::Logs) {
        freeze_logs(chunk, &opts).await
    }
}

async fn freeze_blocks_and_transactions_chunk(chunk: &BlockChunk, opts: &FreezeOpts) {
    let block_numbers = block_utils::get_chunk_block_numbers(&chunk);
    let (blocks, txs) = gather::get_blocks_and_transactions(block_numbers, &opts)
        .await
        .unwrap();
    save_blocks(blocks, &chunk, &opts);
    save_transactions(txs, &chunk, &opts);
}

async fn freeze_blocks_chunk(chunk: &BlockChunk, opts: &FreezeOpts) {
    let block_numbers = block_utils::get_chunk_block_numbers(&chunk);
    let blocks = gather::get_blocks(block_numbers, &opts).await.unwrap();
    save_blocks(blocks, &chunk, &opts);
}

async fn freeze_transactions_chunk(chunk: &BlockChunk, opts: &FreezeOpts) {
    let block_numbers = block_utils::get_chunk_block_numbers(&chunk);
    let txs = gather::get_transactions(block_numbers, &opts)
        .await
        .unwrap();
    save_transactions(txs, &chunk, &opts);
}

async fn freeze_logs(chunk: &BlockChunk, opts: &FreezeOpts) {
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
    let filename = format!(
        "{}__{}__{}.{}",
        opts.network_name, name, block_chunk_stub, opts.output_format.as_str()
    );
    match opts.output_dir.as_str() {
        "." => filename,
        output_dir => output_dir.to_string() + "/" + filename.as_str(),
    }
}

fn save_blocks(blocks: Vec<SlimBlock>, chunk: &BlockChunk, opts: &FreezeOpts) {
    let path = get_file_path("blocks", chunk, &opts);
    let df: &mut DataFrame = &mut dataframes::blocks_to_df(blocks).unwrap();
    dataframes::df_to_file(df, &path);
}

fn save_transactions(txs: Vec<Transaction>, chunk: &BlockChunk, opts: &FreezeOpts) {
    let path = get_file_path("transactions", chunk, &opts);
    let df: &mut DataFrame = &mut dataframes::txs_to_df(txs).unwrap();
    dataframes::df_to_file(df, &path);
}

fn save_logs(logs: Vec<Log>, chunk: &BlockChunk, opts: &FreezeOpts) {
    let path = get_file_path("logs", chunk, &opts);
    let df: &mut DataFrame = &mut dataframes::logs_to_df(logs).unwrap();
    // println!("{:?}", &df.dtypes());
    // *df = dataframes::df_binary_columns_to_hex(&df).unwrap();
    // println!("{:?}", &df.dtypes());
    dataframes::df_to_file(df, &path);
}
