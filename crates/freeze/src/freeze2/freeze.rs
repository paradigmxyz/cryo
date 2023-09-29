// use std::{collections::HashMap, path::PathBuf};

// use futures::stream::FuturesUnordered;
// use futures::StreamExt;

// use crate::freeze2::*;
// use crate::freeze2::{reports, summaries};
// use crate::types::{
//     dataframes, Datatype, ExecutionEnv, FileOutput, FreezeSummary, MetaDatatype, Query, Source,
//     Table,
// };
// use crate::{CollectError, MetaChunk, PartitionLabel};

// type Partition = (
//     MetaDatatype,
//     HashMap<Datatype, PathBuf>,
//     MetaChunk,
//     Source,
//     FileOutput,
//     HashMap<Datatype, Table>,
//     ExecutionEnv,
// );

// type LabeledResult = (PartitionLabel, Result<(), CollectError>);

// pub async fn freeze3(
//     query: &Query,
//     source: &Source,
//     sink: &FileOutput,
//     env: &ExecutionEnv,
// ) -> Result<Option<FreezeSummary>, CollectError> {
//     // get partitions
//     let (partitions, skipping) = get_partitions(query, source, sink, env);

//     // print summary
//     if env.verbose {
//         summaries::print_cryo_intro(&query, &source, &sink, &env, partitions.len() as u64);
//     }

//     // check dry run
//     if env.dry {
//         return Ok(None);
//     };

//     // create initial report
//     if env.report && partitions.len() > 0 {
//         reports::write_report(env, sink, None)?;
//     };

//     // perform collection
//     let futures = FuturesUnordered::new();
//     for (label, partition) in partitions.into_iter() {
//         futures.push(tokio::spawn(async move { (label, freeze_partition(partition).await) }));
//     }
//     let results = aggregate_freeze_results(futures.collect().await, &env, skipping);

//     // create summary
//     if env.verbose {
//         summaries::print_cryo_conclusion(&results, query, env)
//     }

//     // aggregate conclusion
//     Ok(Some(results))
// }

// fn get_partitions(
//     query: &Query,
//     source: &Source,
//     sink: &FileOutput,
//     env: &ExecutionEnv,
// ) -> (Vec<(PartitionLabel, Partition)>, Vec<PartitionLabel>) {
//     let mut outputs = Vec::new();
//     let mut skipping = Vec::new();
//     for (label, dt, chunk) in query.partitions().into_iter() {
//         // get paths
//         let paths: HashMap<Datatype, PathBuf> = dt
//             .datatypes()
//             .iter()
//             .map(|dt| (dt.clone(), sink.get_path(dt.clone(), &chunk, source)))
//             .collect();

//         // get label
//         let label = if let Some(label) = label {
//             PartitionLabel { paths: Some(paths), ..label }
//         } else {
//             PartitionLabel { name: None, datatype: Some(dt.clone()), paths: Some(paths) }
//         };

//         // create partition
//         if paths.values().all(|path| path.exists()) {
//             skipping.push(label);
//             continue;
//         }
//         let partition = (
//             dt,
//             paths,
//             chunk.clone(),
//             source.clone(),
//             sink.clone(),
//             query.schemas.clone(),
//             env.clone(),
//         );
//         outputs.push((label, partition));
//     }
//     (outputs, skipping)
// }

// async fn freeze_partition(partition: Partition) -> Result<(), CollectError> {
//     let (datatype, paths, meta_chunk, source, sink, schema, env) = partition;
//     let dfs = collect_by_block(datatype, meta_chunk, source, schema).await?;
//     for (datatype, mut df) in dfs {
//         let path = paths.get(&datatype).ok_or_else(|| {
//             CollectError::CollectError("could not get path for datatype".to_string())
//         })?;
//         let result = dataframes::df_to_file(&mut df, &path, &sink);
//         result.map_err(|x| CollectError::CollectError("error writing file".to_string()))?
//     }
//     if let Some(bar) = env.bar {
//         bar.inc(1);
//     }
//     Ok(())
// }

// fn aggregate_freeze_results(
//     results: Vec<Result<LabeledResult, tokio::task::JoinError>>,
//     env: &ExecutionEnv,
//     skipping: Vec<PartitionLabel>,
// ) -> FreezeSummary {
//     let mut completed = Vec::new();
//     let mut errored = Vec::new();

//     for result in results.into_iter() {
//         match result.map_err(|_| CollectError::CollectError("tokio join error".to_string())) {
//             Ok((label, Ok(()))) => completed.push(label),
//             Ok((label, Err(_e))) => errored.push(Some(label)),
//             Err(_e) => errored.push(None),
//         }
//     }

//     FreezeSummary { completed, errored, skipping }
// }
