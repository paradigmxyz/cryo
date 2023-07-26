use std::{collections::HashMap, path::Path, sync::Arc};

use futures::future::join_all;
use indicatif::ProgressBar;
use tokio::sync::Semaphore;

use crate::types::{
    dataframes, Chunk, Datatype, FileOutput, FreezeChunkSummary, FreezeError, FreezeSummary,
    FreezeSummaryAgg, MultiDatatype, MultiQuery, Source,
};

/// perform a bulk data extraction of multiple datatypes over multiple block chunks
pub async fn freeze(
    query: &MultiQuery,
    source: &Source,
    sink: &FileOutput,
) -> Result<FreezeSummary, FreezeError> {
    // create progress bar
    let bar = Arc::new(ProgressBar::new(query.chunks.len() as u64));
    bar.set_style(
        indicatif::ProgressStyle::default_bar()
            .template("{wide_bar:.green} {human_pos} / {human_len}   ETA={eta_precise} ")
            .map_err(FreezeError::ProgressBarError)?,
    );

    // freeze chunks concurrently
    let (datatypes, multi_datatypes) = cluster_datatypes(query.schemas.keys().collect());
    let sem = Arc::new(Semaphore::new(source.max_concurrent_chunks as usize));
    let query = Arc::new(query.clone());
    let source = Arc::new(source.clone());
    let sink = Arc::new(sink.clone());
    let mut tasks: Vec<_> = vec![];
    for chunk in query.chunks.clone().into_iter() {
        // datatypes
        for datatype in &datatypes {
            let task = tokio::spawn(freeze_datatype_chunk(
                chunk.clone(),
                *datatype,
                Arc::clone(&sem),
                Arc::clone(&query),
                Arc::clone(&source),
                Arc::clone(&sink),
                Arc::clone(&bar),
            ));
            tasks.push(task)
        }

        // multi datatypes
        for multi_datatype in &multi_datatypes {
            let bar = Arc::clone(&bar);
            let task = tokio::spawn(freeze_multi_datatype_chunk(
                chunk.clone(),
                *multi_datatype,
                Arc::clone(&sem),
                Arc::clone(&query),
                Arc::clone(&source),
                Arc::clone(&sink),
                Arc::clone(&bar),
            ));
            tasks.push(task)
        }
    }
    let chunk_summaries: Vec<FreezeChunkSummary> =
        join_all(tasks).await.into_iter().filter_map(Result::ok).collect();
    Ok(chunk_summaries.aggregate())
}

fn cluster_datatypes(dts: Vec<&Datatype>) -> (Vec<Datatype>, Vec<MultiDatatype>) {
    let mdts: Vec<MultiDatatype> = MultiDatatype::variants()
        .iter()
        .filter(|mdt| mdt.multi_dataset().datatypes().iter().all(|x| dts.contains(&x)))
        .cloned()
        .collect();
    let mdt_dts: Vec<Datatype> =
        mdts.iter().flat_map(|mdt| mdt.multi_dataset().datatypes()).collect();
    let other_dts = dts.iter().filter(|dt| !mdt_dts.contains(dt)).map(|x| **x).collect();
    (other_dts, mdts)
}

async fn freeze_datatype_chunk(
    chunk: Chunk,
    datatype: Datatype,
    sem: Arc<Semaphore>,
    query: Arc<MultiQuery>,
    source: Arc<Source>,
    sink: Arc<FileOutput>,
    bar: Arc<ProgressBar>,
) -> FreezeChunkSummary {
    let _permit = sem.acquire().await.expect("Semaphore acquire");

    let ds = datatype.dataset();

    // create path
    let path = match chunk.filepath(ds.name(), &sink) {
        Err(_e) => return FreezeChunkSummary::error(HashMap::new()),
        Ok(path) => path,
    };
    let paths = HashMap::from([(datatype, path.clone())]);

    // skip path if file already exists
    if Path::new(&path).exists() && !sink.overwrite {
        return FreezeChunkSummary::skip(paths)
    }

    // collect data
    let schema = match query.schemas.get(&datatype) {
        Some(schema) => schema,
        _ => return FreezeChunkSummary::error(paths),
    };
    let collect_output =
        ds.collect_chunk(&chunk, &source, schema, query.row_filters.get(&datatype)).await;
    let mut df = match collect_output {
        Err(_e) => {
            println!("chunk failed: {:?}", _e);
            return FreezeChunkSummary::error(paths)
        }
        Ok(df) => df,
    };

    // write data
    if let Err(_e) = dataframes::df_to_file(&mut df, &path, &sink) {
        return FreezeChunkSummary::error(paths)
    }

    bar.inc(1);
    FreezeChunkSummary::success(paths)
}

async fn freeze_multi_datatype_chunk(
    chunk: Chunk,
    mdt: MultiDatatype,
    sem: Arc<Semaphore>,
    query: Arc<MultiQuery>,
    source: Arc<Source>,
    sink: Arc<FileOutput>,
    bar: Arc<ProgressBar>,
) -> FreezeChunkSummary {
    let _permit = sem.acquire().await.expect("Semaphore acquire");

    // create paths
    let mut paths: HashMap<Datatype, String> = HashMap::new();
    for ds in mdt.multi_dataset().datasets().values() {
        match chunk.filepath(ds.name(), &sink) {
            Err(_e) => return FreezeChunkSummary::error(paths),
            Ok(path) => paths.insert(ds.datatype(), path),
        };
    }

    // skip path if file already exists
    if paths.values().all(|path| Path::new(&path).exists()) && !sink.overwrite {
        return FreezeChunkSummary::skip(paths)
    }

    // collect data
    let collect_result = mdt
        .multi_dataset()
        .collect_chunk(&chunk, &source, query.schemas.clone(), HashMap::new())
        .await;
    let mut dfs = match collect_result {
        Err(_e) => {
            println!("chunk failed: {:?}", _e);
            return FreezeChunkSummary::error(paths)
        }
        Ok(dfs) => dfs,
    };

    // write data
    if let Err(_e) = dataframes::dfs_to_files(&mut dfs, &paths, &sink) {
        return FreezeChunkSummary::error(paths)
    }

    bar.inc(1);
    FreezeChunkSummary::success(paths)
}
