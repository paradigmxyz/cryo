use std::error::Error;
use std::sync::Arc;

use ethers::prelude::*;
use futures::future::try_join_all;
use indicatif::ProgressBar;
use polars::prelude::*;
use tokio::sync::Semaphore;

use crate::chunks;
use crate::datatypes;
use crate::outputs;
use crate::types::{BlockChunk, Datatype, FreezeOpts, SlimBlock};

pub async fn freeze(opts: FreezeOpts) -> Result<(), Box<dyn Error>> {
    // create progress bar
    let bar = Arc::new(ProgressBar::new(opts.block_chunks.len() as u64));
    bar.set_style(
        indicatif::ProgressStyle::default_bar()
            .template("{wide_bar:.green} {human_pos} / {human_len}   ETA={eta_precise} ")
            .unwrap(),
    );

    // freeze chunks concurrently
    let sem = Arc::new(Semaphore::new(opts.max_concurrent_chunks as usize));
    let opts = Arc::new(opts);
    let tasks: Vec<_> = opts
        .block_chunks
        .clone()
        .into_iter()
        .map(|chunk| {
            let sem = Arc::clone(&sem);
            let opts = Arc::clone(&opts);
            let bar = Arc::clone(&bar);
            tokio::spawn(async move {
                let permit = sem.acquire().await.expect("Semaphore acquire");
                freeze_chunk(&chunk, &opts).await;
                drop(permit);
                bar.inc(1);
            })
        })
        .collect();

    // gather results
    let _results = try_join_all(tasks).await?;
    Ok(())
}

async fn freeze_chunk(chunk: &BlockChunk, opts: &FreezeOpts) {
    let dts = &opts.datatypes;
    if dts.contains(&Datatype::Blocks) & dts.contains(&Datatype::Transactions) {
        freeze_blocks_and_transactions_chunk(chunk, &opts).await
    } else if dts.contains(&Datatype::Blocks) {
        freeze_blocks_chunk(chunk, &opts).await
    } else if dts.contains(&Datatype::Transactions) {
        freeze_transactions_chunk(chunk, &opts).await
    }
    if dts.contains(&Datatype::Logs) {
        freeze_logs(chunk, &opts).await
    }
}

async fn freeze_blocks_and_transactions_chunk(chunk: &BlockChunk, opts: &FreezeOpts) {
    let block_numbers = chunks::get_chunk_block_numbers(&chunk);
    let (blocks, txs) = datatypes::blocks::get_blocks_and_transactions(block_numbers, &opts)
        .await
        .unwrap();
    save_blocks(blocks, &chunk, &opts);
    save_transactions(txs, &chunk, &opts);
}

async fn freeze_blocks_chunk(chunk: &BlockChunk, opts: &FreezeOpts) {
    let block_numbers = chunks::get_chunk_block_numbers(&chunk);
    let blocks = datatypes::blocks::get_blocks(block_numbers, &opts)
        .await
        .unwrap();
    save_blocks(blocks, &chunk, &opts);
}

async fn freeze_transactions_chunk(chunk: &BlockChunk, opts: &FreezeOpts) {
    let block_numbers = chunks::get_chunk_block_numbers(&chunk);
    let txs = datatypes::transactions::get_transactions(block_numbers, &opts)
        .await
        .unwrap();
    save_transactions(txs, &chunk, &opts);
}

async fn freeze_logs(chunk: &BlockChunk, opts: &FreezeOpts) {
    let logs = datatypes::logs::get_logs(&chunk, None, [None, None, None, None], &opts)
        .await
        .unwrap();
    save_logs(logs, &chunk, &opts);
}

// saving

fn save_blocks(blocks: Vec<SlimBlock>, chunk: &BlockChunk, opts: &FreezeOpts) {
    let path = outputs::get_chunk_path("blocks", chunk, &opts);
    let df: &mut DataFrame = &mut datatypes::blocks::blocks_to_df(blocks).unwrap();
    outputs::df_to_file(df, &path);
}

fn save_transactions(txs: Vec<Transaction>, chunk: &BlockChunk, opts: &FreezeOpts) {
    let path = outputs::get_chunk_path("transactions", chunk, &opts);
    let df: &mut DataFrame = &mut datatypes::transactions::txs_to_df(txs).unwrap();
    outputs::df_to_file(df, &path);
}

fn save_logs(logs: Vec<Log>, chunk: &BlockChunk, opts: &FreezeOpts) {
    let path = outputs::get_chunk_path("logs", chunk, &opts);
    let df: &mut DataFrame = &mut datatypes::logs::logs_to_df(logs).unwrap();
    outputs::df_to_file(df, &path);
}
