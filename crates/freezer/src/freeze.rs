use std::path::Path;
use std::sync::Arc;

use futures::future::try_join_all;
use indicatif::ProgressBar;
use tokio::sync::Semaphore;

use crate::outputs;
use crate::types::BlockChunk;
use crate::types::FreezeOpts;
use crate::types::FreezeSummary;
use crate::types::FreezeChunkSummary;

pub async fn freeze(opts: FreezeOpts) -> Result<FreezeSummary, Box<dyn std::error::Error>> {
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
    let chunk_summaries: Vec<FreezeChunkSummary> = try_join_all(tasks).await.unwrap();
    Ok(create_freeze_summary(chunk_summaries))
}

async fn freeze_chunk(
    block_chunk: BlockChunk,
    sem: Arc<Semaphore>,
    opts: Arc<FreezeOpts>,
    bar: Arc<ProgressBar>,
) -> FreezeChunkSummary {
    let permit = sem.acquire().await.expect("Semaphore acquire");
    for dt in &opts.datatypes {
        let ds = dt.dataset();
        let path = outputs::get_chunk_path(ds.name(), &block_chunk, &opts);

        if Path::new(&path).exists() & !opts.overwrite {
            return FreezeChunkSummary { skipped: true };
        } else {
            let mut df = ds.collect_chunk(&block_chunk, &opts).await.unwrap();
            outputs::df_to_file(&mut df, &path, &opts);
        }
    }
    drop(permit);
    bar.inc(1);

    FreezeChunkSummary { skipped: false }
}

fn create_freeze_summary(chunk_summaries: Vec<FreezeChunkSummary>) -> FreezeSummary {
    let mut n_completed: u64 = 0;
    let mut n_skipped: u64 = 0;

    for chunk_sumary in chunk_summaries {
        if chunk_sumary.skipped {
            n_skipped += 1;
        } else {
            n_completed += 1;
        }
    }

    FreezeSummary {
        n_completed,
        n_skipped,
    }
}
