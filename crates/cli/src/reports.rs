use crate::args;
use chrono::{DateTime, Local};
use cryo_freeze::{FreezeError, FreezeSummary};
use std::{fs::File, io::Write, path::Path, time::SystemTime};

#[derive(serde::Serialize, Debug)]
struct FreezeReport<'a> {
    cryo_version: String,
    // node_client: String,
    cli_command: Vec<String>,
    args: &'a args::Args,
    chunk_summary: Option<&'a FreezeSummary>,
}

pub(crate) fn get_report_path(
    args: &args::Args,
    t_start: SystemTime,
    is_complete: bool,
) -> Result<String, FreezeError> {
    let report_dir = match &args.report_dir {
        Some(report_dir) => Path::new(&report_dir).into(),
        None => Path::new(&args.output_dir).join(".cryo_reports"),
    };
    std::fs::create_dir_all(&report_dir)?;
    let t_start: DateTime<Local> = t_start.into();
    let timestamp: String = t_start.format("%Y-%m-%d_%H-%M-%S").to_string();
    let filename = if is_complete {
        timestamp + ".json"
    } else {
        format!("incomplete_{}", timestamp + ".json")
    };
    let path = report_dir.join(filename);
    path.to_str()
        .ok_or(FreezeError::GeneralError("non-String path".to_string()))
        .map(|s| s.to_string())
}

pub(crate) fn write_report(
    args: &args::Args,
    freeze_summary: Option<&FreezeSummary>,
    t_start: SystemTime,
) -> Result<String, FreezeError> {
    // determine version
    let cryo_version = format!("{}__{}", env!("CARGO_PKG_VERSION"), env!("GIT_DESCRIPTION"));
    let report = FreezeReport {
        cryo_version,
        cli_command: std::env::args().collect(),
        args,
        chunk_summary: freeze_summary,
    };
    let serialized = serde_json::to_string(&report)?;

    // create path
    let path = get_report_path(args, t_start, freeze_summary.is_some())?;

    // save to file
    let mut file = File::create(&path)?;
    file.write_all(serialized.as_bytes())?;

    Ok(path)
}
