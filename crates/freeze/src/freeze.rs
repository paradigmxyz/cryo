use crate::{
    collect_partition, dataframes, err, reports, summaries, CollectError, Datatype, ExecutionEnv,
    FileOutput, FreezeSummary, MetaDatatype, Partition, Query, Source,
};
use chrono::{DateTime, Local};
use futures::{stream::FuturesUnordered, StreamExt};
use std::{
    collections::{HashMap, HashSet},
    path::PathBuf,
    sync::Arc,
};
use tokio::sync::Semaphore;

type PartitionPayload = (
    Partition,
    MetaDatatype,
    HashMap<Datatype, PathBuf>,
    Arc<Query>,
    Arc<Source>,
    FileOutput,
    ExecutionEnv,
    Option<std::sync::Arc<Semaphore>>,
);

/// collect data and output as files
pub async fn freeze(
    query: &Query,
    source: &Source,
    sink: &FileOutput,
    env: &ExecutionEnv,
) -> Result<Option<FreezeSummary>, CollectError> {
    // check validity of query
    query.is_valid()?;

    // get partitions
    let (payloads, skipping) = get_payloads(query, source, sink, env)?;

    // print summary
    if env.verbose >= 1 {
        summaries::print_cryo_intro(query, source, sink, env, payloads.len() as u64)?;
    }

    // check dry run
    if env.dry {
        return Ok(None)
    };

    // check if empty
    if payloads.is_empty() {
        let results = FreezeSummary { skipped: skipping, ..Default::default() };
        if env.verbose >= 1 {
            summaries::print_cryo_conclusion(&results, query, env)
        }
        return Ok(Some(results))
    }

    // create initial report
    if env.report {
        reports::write_report(env, query, sink, None)?;
    };

    // perform collection
    let results = freeze_partitions(env, payloads, skipping).await;

    // create summary
    if env.verbose >= 1 {
        summaries::print_cryo_conclusion(&results, query, env)
    }

    // create final report
    if env.report {
        reports::write_report(env, query, sink, Some(&results))?;
    };

    // return
    Ok(Some(results))
}

fn get_payloads(
    query: &Query,
    source: &Source,
    sink: &FileOutput,
    env: &ExecutionEnv,
) -> Result<(Vec<PartitionPayload>, Vec<Partition>), CollectError> {
    let semaphore = source
        .max_concurrent_chunks
        .map(|x| std::sync::Arc::new(tokio::sync::Semaphore::new(x as usize)));
    let source: Arc<Source> = Arc::new(source.clone());
    let arc_query = Arc::new(query.clone());
    let mut payloads = Vec::new();
    let mut skipping = Vec::new();
    let mut all_paths = HashSet::new();
    for datatype in query.datatypes.clone().into_iter() {
        for partition in query.partitions.clone().into_iter() {
            let paths = sink.get_paths(query, &partition, Some(vec![datatype.clone()]))?;
            if !sink.overwrite && paths.values().all(|path| path.exists()) {
                skipping.push(partition);
                continue
            }

            // check for path collisions
            let paths_set: HashSet<_> = paths.clone().into_values().collect();
            if paths_set.intersection(&all_paths).next().is_none() {
                all_paths.extend(paths_set);
            } else {
                let message =
                    format!("output path collision: {:?}", paths_set.intersection(&all_paths));
                return Err(err(&message))
            };

            let payload = (
                partition.clone(),
                datatype.clone(),
                paths,
                arc_query.clone(),
                source.clone(),
                sink.clone(),
                env.clone(),
                semaphore.clone(),
            );
            payloads.push(payload);
        }
    }
    Ok((payloads, skipping))
}

async fn freeze_partitions(
    env: &ExecutionEnv,
    payloads: Vec<PartitionPayload>,
    skipped: Vec<Partition>,
) -> FreezeSummary {
    if let Some(bar) = &env.bar {
        bar.set_length(payloads.len() as u64);
        if let Some(payload) = &payloads.first() {
            let (_, _, _, _, _, _, env, _) = payload;
            let dt_start: DateTime<Local> = env.t_start.into();
            bar.set_message(format!("started at {}", dt_start.format("%Y-%m-%d %H:%M:%S%.3f")));
        }
    }

    // spawn task for each partition
    let mut futures = FuturesUnordered::new();
    for payload in payloads.into_iter() {
        futures.push(tokio::spawn(
            async move { (payload.0.clone(), freeze_partition(payload).await) },
        ));
    }

    // aggregate results
    let mut completed = Vec::new();
    let mut errored = Vec::new();
    let mut n_rows = 0;
    while let Some(result) = futures.next().await {
        match result {
            Ok((partition, Ok(chunk_n_rows))) => {
                n_rows += chunk_n_rows;
                completed.push(partition)
            }
            Ok((partition, Err(e))) => errored.push((Some(partition), e)),
            Err(e) => errored.push((None, err(format!("error joining chunks: {:?}", e).as_str()))),
        }
    }

    if let Some(bar) = &env.bar {
        bar.finish_and_clear();
    }

    FreezeSummary { completed, errored, skipped, n_rows }
}

async fn freeze_partition(payload: PartitionPayload) -> Result<u64, CollectError> {
    let (partition, datatype, paths, query, source, sink, env, semaphore) = payload;

    // acquire chunk semaphore
    let _permit = match &semaphore {
        Some(semaphore) => Some(semaphore.acquire().await),
        None => None,
    };

    // collect data
    let dfs = collect_partition(datatype, partition, query, source).await?;

    // write dataframes to disk
    let mut n_rows = 0;
    for (datatype, mut df) in dfs {
        let df_height = df.height() as u64;
        if env.skip_empty == Some(true) && df_height == 0 {
            continue;
        };
        n_rows += df_height;
        let path = paths.get(&datatype).ok_or_else(|| {
            CollectError::CollectError("could not get path for datatype".to_string())
        })?;
        let result = dataframes::df_to_file(&mut df, path, &sink);
        result.map_err(|_| CollectError::CollectError("error writing file".to_string()))?
    }

    // update progress bar
    if let Some(bar) = env.bar {
        bar.inc(1);
    }

    Ok(n_rows)
}
