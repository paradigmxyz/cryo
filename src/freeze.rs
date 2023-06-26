use std::sync::Arc;

use futures::future::try_join_all;
use indicatif::ProgressBar;
use tokio::sync::Semaphore;

use crate::outputs;
use crate::types::FreezeOpts;

pub async fn freeze(opts: FreezeOpts) {
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
                for dt in &opts.datatypes {
                    let ds = dt.get_dataset();
                    let mut df = ds.collect_dataset(&chunk, &opts).await;

                    // save
                    let path = outputs::get_chunk_path(ds.name(), &chunk, &opts);
                    outputs::df_to_file(&mut df, &path);
                }
                drop(permit);
                bar.inc(1);
            })
        })
        .collect();

    // gather results
    let _results = try_join_all(tasks).await;
}
