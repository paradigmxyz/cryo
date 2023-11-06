use crate::{err, CollectError, ExecutionEnv, FreezeSummary, Query, Sink};
use chrono::{DateTime, Local};
use std::{fs::File, io::Write, path::PathBuf, sync::Arc};

#[derive(serde::Serialize, Debug)]
struct FreezeReport {
    cryo_version: String,
    // node_client: String,
    cli_command: Option<Vec<String>>,
    results: Option<SerializedFreezeSummary>,
    args: Option<String>,
}

#[derive(serde::Serialize, Debug)]
struct SerializedFreezeSummary {
    completed_paths: Vec<String>,
    errored_paths: Vec<String>,
    n_skipped: u64,
}

pub(crate) fn get_report_path(
    env: &ExecutionEnv,
    is_complete: bool,
) -> Result<PathBuf, CollectError> {
    let report_dir = match env.report_dir.clone() {
        Some(report_dir) => report_dir,
        None => return Err(err("report dir unspecified")),
    };
    std::fs::create_dir_all(&report_dir)
        .map_err(|_| CollectError::CollectError("could not create report dir".to_string()))?;

    // create file name
    let t_start: DateTime<Local> = env.t_start.into();
    let timestamp: String = t_start.format("%Y-%m-%d_%H-%M-%S%.6f").to_string();
    let filename = if is_complete {
        timestamp + ".json"
    } else {
        format!("incomplete_{}", timestamp + ".json")
    };

    // create and return path
    Ok(report_dir.join(filename))
}

pub(crate) fn write_report(
    env: &ExecutionEnv,
    query: &Query,
    sink: &Arc<dyn Sink>,
    freeze_summary: Option<&FreezeSummary>,
) -> Result<PathBuf, CollectError> {
    // determine version
    let cryo_version = CRYO_VERSION.to_string();
    let serialized_summary = match freeze_summary {
        Some(x) => Some(serialize_summary(x, query, sink)?),
        None => None,
    };
    let report = FreezeReport {
        cryo_version,
        cli_command: env.cli_command.clone(),
        args: env.args.clone(),
        results: serialized_summary,
    };
    let serialized = serde_json::to_string(&report)
        .map_err(|_| CollectError::CollectError("could not serialize report".to_string()))?;

    // create path
    let path = get_report_path(env, freeze_summary.is_some())?;

    // save to file
    let mut file = File::create(&path)
        .map_err(|_| CollectError::CollectError("could not create report file".to_string()))?;
    file.write_all(serialized.as_bytes())
        .map_err(|_| CollectError::CollectError("could not write report data".to_string()))?;

    // delete initial report
    if freeze_summary.is_some() {
        let incomplete_path = get_report_path(env, false)?;
        std::fs::remove_file(incomplete_path)
            .map_err(|_| err("could not delete initial report file"))?;
    }

    Ok(path)
}

fn serialize_summary(
    summary: &FreezeSummary,
    query: &Query,
    sink: &Arc<dyn Sink>,
) -> Result<SerializedFreezeSummary, CollectError> {
    let completed_paths: Vec<String> = summary
        .completed
        .iter()
        .map(|partition| {
            sink.get_ids(query, partition, None)
                .map(|paths| paths.values().cloned().collect::<Vec<_>>())
        })
        .collect::<Result<Vec<_>, _>>()?
        .into_iter()
        .flatten()
        .collect();

    let errored_paths: Vec<String> = summary
        .errored
        .iter()
        .filter_map(|(partition_option, _error)| {
            partition_option.as_ref().map(|partition| {
                sink.get_ids(query, partition, None)
                    .map(|paths| paths.values().cloned().collect::<Vec<_>>())
            })
        })
        .collect::<Result<Vec<_>, _>>()?
        .into_iter()
        .flatten()
        .collect();

    Ok(SerializedFreezeSummary {
        completed_paths,
        errored_paths,
        n_skipped: summary.skipped.len() as u64,
    })
}

/// cryo version
pub const CRYO_VERSION: &str = env!("GIT_DESCRIPTION");
