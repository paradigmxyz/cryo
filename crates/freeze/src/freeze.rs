use crate::{
    collect_partition, dataframes, reports, summaries, CollectError, Datatype, ExecutionEnv,
    FileOutput, FreezeSummary, MetaDatatype, Partition, Query, Source, Table, TimeDimension,
};
use futures::{stream::FuturesUnordered, StreamExt};
use std::{collections::HashMap, path::PathBuf};

type PartitionPayload = (
    TimeDimension,
    Partition,
    MetaDatatype,
    HashMap<Datatype, PathBuf>,
    Source,
    FileOutput,
    HashMap<Datatype, Table>,
    ExecutionEnv,
);

/// collect data and output as files
pub async fn freeze(
    query: &Query,
    source: &Source,
    sink: &FileOutput,
    env: &ExecutionEnv,
) -> Result<Option<FreezeSummary>, CollectError> {
    // // check validity of query
    // query.is_valid()?;

    // get partitions
    let (payloads, skipping) = get_payloads(query, source, sink, env)?;

    // print summary
    if env.verbose {
        summaries::print_cryo_intro(query, source, sink, env, payloads.len() as u64);
    }

    // check dry run
    if env.dry {
        return Ok(None)
    };

    // check if empty
    if payloads.is_empty() {
        return Ok(Some(FreezeSummary { ..Default::default() }))
    }

    // create initial report
    if env.report {
        reports::write_report(env, query, sink, None)?;
    };

    // perform collection
    let results = perform_freeze(env, payloads, skipping).await;

    // create summary
    if env.verbose {
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
    let mut payloads = Vec::new();
    let mut skipping = Vec::new();
    for datatype in query.datatypes.clone().into_iter() {
        for partition in query.partitions.clone().into_iter() {
            let paths = sink.get_paths(query, &partition)?;
            if !sink.overwrite && paths.values().all(|path| path.exists()) {
                skipping.push(partition);
                continue
            }
            let payload = (
                query.time_dimension.clone(),
                partition.clone(),
                datatype.clone(),
                paths,
                source.clone(),
                sink.clone(),
                query.schemas.clone(),
                env.clone(),
            );
            payloads.push(payload);
        }
    }
    Ok((payloads, skipping))
}

async fn perform_freeze(
    env: &ExecutionEnv,
    payloads: Vec<PartitionPayload>,
    skipped: Vec<Partition>,
) -> FreezeSummary {
    if let Some(bar) = &env.bar {
        bar.inc(0);
    }

    // spawn task for each partition
    let mut futures = FuturesUnordered::new();
    for payload in payloads.into_iter() {
        futures.push(tokio::spawn(
            async move { (payload.1.clone(), freeze_partition(payload).await) },
        ));
    }

    let mut errors = Vec::new();

    // aggregate results
    let mut completed = Vec::new();
    let mut errored = Vec::new();
    while let Some(result) = futures.next().await {
        match result {
            Ok((partition, Ok(()))) => completed.push(partition),
            Ok((partition, Err(e))) => {
                errors.push(e);
                errored.push(Some(partition))
            }
            Err(_e) => errored.push(None),
        }
    }

    if let Some(bar) = &env.bar {
        bar.finish_and_clear();
    }

    if !errors.is_empty() {
        println!();
        println!("ERRORS");
        for error in errors.iter() {
            println!("{}", error)
        }
        println!();
    }

    FreezeSummary { completed, errored, skipped }
}

async fn freeze_partition(payload: PartitionPayload) -> Result<(), CollectError> {
    let (time_dimension, partition, datatype, paths, source, sink, schemas, env) = payload;
    let dfs = collect_partition(time_dimension, datatype, partition, source, schemas).await?;
    for (datatype, mut df) in dfs {
        let path = paths.get(&datatype).ok_or_else(|| {
            CollectError::CollectError("could not get path for datatype".to_string())
        })?;
        let result = dataframes::df_to_file(&mut df, path, &sink);
        result.map_err(|_| CollectError::CollectError("error writing file".to_string()))?
    }
    if let Some(bar) = env.bar {
        bar.inc(1);
    }
    Ok(())
}
