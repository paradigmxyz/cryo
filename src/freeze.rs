use std::sync::Arc;

use futures::future::try_join_all;
use indicatif::ProgressBar;
use tokio::sync::Semaphore;

use crate::outputs;
use crate::types::BlockChunk;
use crate::types::FreezeOpts;

pub async fn freeze(opts: FreezeOpts) -> Result<Vec<()>, Box<dyn std::error::Error>> {
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
    let mut tasks: Vec<_> = vec![];
    for block_chunk in opts.block_chunks.clone().into_iter() {
        let sem = Arc::clone(&sem);
        let opts = Arc::clone(&opts);
        let bar = Arc::clone(&bar);
        let task = tokio::spawn(freeze_chunk(block_chunk, sem, opts, bar));
        tasks.push(task)
    }
    try_join_all(tasks).await.map_err(|e| e.into())
}

async fn freeze_chunk(
    block_chunk: BlockChunk,
    sem: Arc<Semaphore>,
    opts: Arc<FreezeOpts>,
    bar: Arc<ProgressBar>,
) {
    let permit = sem.acquire().await.expect("Semaphore acquire");
    for dt in &opts.datatypes {
        let ds = dt.dataset();
        let mut df = ds.collect_dataset(&block_chunk, &opts).await;
        let path = outputs::get_chunk_path(ds.name(), &block_chunk, &opts);
        outputs::df_to_file(&mut df, &path);
    }
    drop(permit);
    bar.inc(1);
}

