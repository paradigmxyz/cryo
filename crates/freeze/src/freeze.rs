use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;

use futures::future::join_all;
use indicatif::ProgressBar;
use tokio::sync::Semaphore;

use crate::types::dataframes;
use crate::types::Chunk;
use crate::types::Datatype;
use crate::types::FreezeChunkSummary;
use crate::types::FreezeError;
use crate::types::FreezeOpts;
use crate::types::FreezeSummary;
use crate::types::FreezeSummaryAgg;
use crate::types::MultiDatatype;

/// perform a bulk data extraction of multiple datatypes over multiple block chunks
pub async fn freeze(opts: FreezeOpts) -> Result<FreezeSummary, FreezeError> {
    // create progress bar
    let bar = Arc::new(ProgressBar::new(opts.chunks.len() as u64));
    bar.set_style(
        indicatif::ProgressStyle::default_bar()
            .template("{wide_bar:.green} {human_pos} / {human_len}   ETA={eta_precise} ")
            .map_err(FreezeError::ProgressBarError)?,
    );

    // freeze chunks concurrently
    let (datatypes, multi_datatypes) = cluster_datatypes(&opts.datatypes);
    let sem = Arc::new(Semaphore::new(opts.max_concurrent_chunks as usize));
    let opts = Arc::new(opts);
    let mut tasks: Vec<_> = vec![];
    for chunk in opts.chunks.clone().into_iter() {
        // datatypes
        for datatype in &datatypes {
            let sem = Arc::clone(&sem);
            let opts = Arc::clone(&opts);
            let bar = Arc::clone(&bar);
            let task = tokio::spawn(freeze_datatype_chunk(
                chunk.clone(),
                *datatype,
                sem,
                opts,
                bar,
            ));
            tasks.push(task)
        }

        // multi datatypes
        for multi_datatype in &multi_datatypes {
            let sem = Arc::clone(&sem);
            let opts = Arc::clone(&opts);
            let bar = Arc::clone(&bar);
            let task = tokio::spawn(freeze_multi_datatype_chunk(
                chunk.clone(),
                *multi_datatype,
                sem,
                opts,
                bar,
            ));
            tasks.push(task)
        }
    }
    let chunk_summaries: Vec<FreezeChunkSummary> = join_all(tasks)
        .await
        .into_iter()
        .filter_map(Result::ok)
        .collect();
    Ok(chunk_summaries.aggregate())
}

fn cluster_datatypes(dts: &[Datatype]) -> (Vec<Datatype>, Vec<MultiDatatype>) {
    let mdts: Vec<MultiDatatype> = MultiDatatype::variants()
        .iter()
        .filter(|mdt| {
            mdt.multi_dataset()
                .datatypes()
                .iter()
                .all(|x| dts.contains(x))
        })
        .cloned()
        .collect();
    let mdt_dts: Vec<Datatype> = mdts
        .iter()
        .flat_map(|mdt| mdt.multi_dataset().datatypes())
        .collect();
    let other_dts = dts
        .iter()
        .filter(|dt| !mdt_dts.contains(dt))
        .cloned()
        .collect();
    (other_dts, mdts)
}

async fn freeze_datatype_chunk(
    chunk: Chunk,
    datatype: Datatype,
    sem: Arc<Semaphore>,
    opts: Arc<FreezeOpts>,
    bar: Arc<ProgressBar>,
) -> FreezeChunkSummary {
    let _permit = sem.acquire().await.expect("Semaphore acquire");

    let ds = datatype.dataset();

    // create path
    let path = match chunk.filepath(ds.name(), &opts) {
        Err(_e) => return FreezeChunkSummary::error(),
        Ok(path) => path,
    };

    // skip path if file already exists
    if Path::new(&path).exists() && !opts.overwrite {
        return FreezeChunkSummary::skip();
    }

    // collect data
    let schema = match opts.schemas.get(&datatype) {
        Some(schema) => schema,
        _ => return FreezeChunkSummary::error(),
    };
    let collect_output = ds
        .collect_chunk(
            &chunk,
            &opts.build_source(),
            schema,
            opts.row_filter.as_ref(),
        )
        .await;
    let mut df = match collect_output {
        Err(_e) => {
            println!("chunk failed: {:?}", _e);
            return FreezeChunkSummary::error();
        }
        Ok(df) => df,
    };

    // write data
    if let Err(_e) = dataframes::df_to_file(&mut df, &path, &opts) {
        return FreezeChunkSummary::error();
    }

    bar.inc(1);
    FreezeChunkSummary::success()
}

async fn freeze_multi_datatype_chunk(
    chunk: Chunk,
    mdt: MultiDatatype,
    sem: Arc<Semaphore>,
    opts: Arc<FreezeOpts>,
    bar: Arc<ProgressBar>,
) -> FreezeChunkSummary {
    let _permit = sem.acquire().await.expect("Semaphore acquire");

    // create paths
    let mut paths: HashMap<Datatype, String> = HashMap::new();
    for ds in mdt.multi_dataset().datasets().values() {
        match chunk.filepath(ds.name(), &opts) {
            Err(_e) => return FreezeChunkSummary::error(),
            Ok(path) => paths.insert(ds.datatype(), path),
        };
    }

    // skip path if file already exists
    if paths.values().all(|path| Path::new(&path).exists()) && !opts.overwrite {
        return FreezeChunkSummary::skip();
    }

    // collect data
    let collect_result = mdt
        .multi_dataset()
        .collect_chunk(&chunk, &opts.build_source(), opts.schemas.clone(), HashMap::new())
        .await;
    let mut dfs = match collect_result {
        Err(_e) => {
            println!("chunk failed: {:?}", _e);
            return FreezeChunkSummary::error();
        }
        Ok(dfs) => dfs,
    };

    // write data
    if let Err(_e) = dataframes::dfs_to_files(&mut dfs, &paths, &opts) {
        return FreezeChunkSummary::error();
    }

    bar.inc(1);
    FreezeChunkSummary::success()
}
