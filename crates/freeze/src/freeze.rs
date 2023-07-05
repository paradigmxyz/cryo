use std::path::Path;
use std::sync::Arc;

use futures::future::join_all;
use indicatif::ProgressBar;
use tokio::sync::Semaphore;

use crate::outputs;
use crate::types::BlockChunk;
use crate::types::FreezeChunkSummary;
use crate::types::FreezeError;
use crate::types::FreezeOpts;
use crate::types::FreezeSummary;

/// perform a bulk data extraction of multiple datatypes over multiple block chunks
pub async fn freeze(opts: FreezeOpts) -> Result<FreezeSummary, FreezeError> {
    // create progress bar
    let bar = Arc::new(ProgressBar::new(opts.block_chunks.len() as u64));
    bar.set_style(
        indicatif::ProgressStyle::default_bar()
            .template("{wide_bar:.green} {human_pos} / {human_len}   ETA={eta_precise} ")
            .map_err(FreezeError::ProgressBarError)?,
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
    let chunk_summaries: Vec<FreezeChunkSummary> = join_all(tasks)
        .await
        .into_iter()
        .filter_map(Result::ok)
        .collect();
    Ok(create_freeze_summary(chunk_summaries))
}

/// perform a bulk data extraction of multiple datatypes over a single block chunk
async fn freeze_chunk(
    block_chunk: BlockChunk,
    sem: Arc<Semaphore>,
    opts: Arc<FreezeOpts>,
    bar: Arc<ProgressBar>,
) -> FreezeChunkSummary {
    let _permit = sem.acquire().await.expect("Semaphore acquire");
    for dt in &opts.datatypes {
        let ds = dt.dataset();
        match outputs::get_chunk_path(ds.name(), &block_chunk, &opts) {
            Ok(path) => {
                if Path::new(&path).exists() & !opts.overwrite {
                    return FreezeChunkSummary {
                        skipped: true,
                        errored: false,
                    };
                } else {
                    match ds.collect_chunk(&block_chunk, &opts).await {
                        Ok(mut df) => {
                            if let Err(_e) = outputs::df_to_file(&mut df, &path, &opts) {
                                return FreezeChunkSummary {
                                    skipped: false,
                                    errored: true,
                                };
                            }
                        }
                        Err(_e) => {
                            return FreezeChunkSummary {
                                skipped: false,
                                errored: true,
                            }
                        }
                    }
                }
            }
            _ => {
                return FreezeChunkSummary {
                    skipped: false,
                    errored: true,
                }
            }
        }
    }
    bar.inc(1);
    FreezeChunkSummary {
        skipped: false,
        errored: false,
    }
}

fn create_freeze_summary(chunk_summaries: Vec<FreezeChunkSummary>) -> FreezeSummary {
    let mut n_completed: u64 = 0;
    let mut n_skipped: u64 = 0;
    let mut n_errored: u64 = 0;

    for chunk_summary in chunk_summaries {
        if chunk_summary.skipped {
            n_skipped += 1;
        } else if chunk_summary.errored {
            n_errored += 1;
        } else {
            n_completed += 1;
        }
    }

    FreezeSummary {
        n_completed,
        n_skipped,
        n_errored,
    }
}
