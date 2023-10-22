use crate::{
    collect_partition, err, reports, summaries, CollectError, Datatype, ExecutionEnv,
    FreezeSummary, MetaDatatype, Partition, Query, Sink, Source,
};
use chrono::{DateTime, Local};
use futures::{stream::FuturesUnordered, StreamExt};
use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};
use tokio::sync::Semaphore;

type PartitionPayload = (
    Partition,
    MetaDatatype,
    HashMap<Datatype, String>,
    Arc<Query>,
    Arc<Source>,
    Arc<dyn Sink>,
    ExecutionEnv,
    Option<std::sync::Arc<Semaphore>>,
);

/// collect data and output as files
pub async fn freeze(
    query: &Query,
    source: &Source,
    sink: Arc<dyn Sink>,
    env: &ExecutionEnv,
) -> Result<Option<FreezeSummary>, CollectError> {
    // check validity of query
    query.is_valid()?;

    // get partitions
    let (payloads, skipping) = get_payloads(query, source, &sink, env)?;

    // print summary
    if env.verbose >= 1 {
        summaries::print_cryo_intro(query, source, &sink, env, payloads.len() as u64);
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
        reports::write_report(env, query, &sink, None)?;
    };

    // perform collection
    let results = freeze_partitions(env, payloads, skipping).await;

    // create summary
    if env.verbose >= 1 {
        summaries::print_cryo_conclusion(&results, query, env)
    }

    // create final report
    if env.report {
        reports::write_report(env, query, &sink, Some(&results))?;
    };

    // return
    Ok(Some(results))
}

fn get_payloads(
    query: &Query,
    source: &Source,
    sink: &Arc<dyn Sink>,
    env: &ExecutionEnv,
) -> Result<(Vec<PartitionPayload>, Vec<Partition>), CollectError> {
    let semaphore = source
        .max_concurrent_chunks
        .map(|x| std::sync::Arc::new(tokio::sync::Semaphore::new(x as usize)));
    let source = Arc::new(source.clone());
    let arc_query = Arc::new(query.clone());
    let mut payloads = Vec::new();
    let mut skipping = Vec::new();
    let mut all_ids = HashSet::new();
    for datatype in query.datatypes.clone().into_iter() {
        for partition in query.partitions.clone().into_iter() {
            let ids = sink.get_ids(query, &partition, Some(vec![datatype.clone()]))?;
            if !sink.overwrite() && ids.values().all(|path| sink.exists(path)) {
                skipping.push(partition);
                continue
            }

            // check for path collisions
            let ids_set: HashSet<_> = ids.clone().into_values().collect();
            if ids_set.intersection(&all_ids).next().is_none() {
                all_ids.extend(ids_set);
            } else {
                let message = format!("output id collision: {:?}", ids_set.intersection(&all_ids));
                return Err(err(&message))
            };

            let payload = (
                partition.clone(),
                datatype.clone(),
                ids,
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
    while let Some(result) = futures.next().await {
        match result {
            Ok((partition, Ok(()))) => completed.push(partition),
            Ok((partition, Err(e))) => errored.push((Some(partition), e)),
            Err(_e) => errored.push((None, err("error joining chunks"))),
        }
    }

    if let Some(bar) = &env.bar {
        bar.finish_and_clear();
    }

    FreezeSummary { completed, errored, skipped }
}

async fn freeze_partition(payload: PartitionPayload) -> Result<(), CollectError> {
    let (partition, datatype, ids, query, source, sink, env, semaphore) = payload;

    // acquire chunk semaphore
    let _permit = match &semaphore {
        Some(semaphore) => Some(semaphore.acquire().await),
        None => None,
    };

    // collect data
    let dfs = collect_partition(datatype, partition, query, source).await?;

    // write dataframes to disk
    for (datatype, mut df) in dfs {
        let id = ids.get(&datatype).ok_or_else(|| err("could not get id for datatype"))?;
        sink.sink_df(&mut df, id)?;
    }

    // update progress bar
    if let Some(bar) = env.bar {
        bar.inc(1);
    }

    Ok(())
}
