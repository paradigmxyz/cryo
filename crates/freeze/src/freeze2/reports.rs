// use crate::{CollectError, ExecutionEnv, FileOutput, FreezeSummary};
// use chrono::{DateTime, Local};
// use std::{
//     fs::File,
//     io::Write,
//     path::{Path, PathBuf},
// };

// #[derive(serde::Serialize, Debug)]
// struct FreezeReport<'a> {
//     cryo_version: String,
//     // node_client: String,
//     cli_command: Option<Vec<String>>,
//     results: Option<&'a FreezeSummary>,
//     args: Option<String>,
// }

// pub(crate) fn get_report_path(
//     env: &ExecutionEnv,
//     sink: &FileOutput,
//     is_complete: bool,
// ) -> Result<PathBuf, CollectError> {
//     // create directory
//     let report_dir = match &env.report_dir {
//         Some(report_dir) => Path::new(&report_dir).into(),
//         None => Path::new(&sink.output_dir).join(".cryo/reports"),
//     };
//     std::fs::create_dir_all(&report_dir)
//         .map_err(|_| CollectError::CollectError("could not create report dir".to_string()))?;

//     // create file name
//     let t_start: DateTime<Local> = env.t_start.into();
//     let timestamp: String = t_start.format("%Y-%m-%d_%H-%M-%S").to_string();
//     let filename = if is_complete {
//         timestamp + ".json"
//     } else {
//         format!("incomplete_{}", timestamp + ".json")
//     };

//     // create and return path
//     Ok(report_dir.join(filename))
// }

// pub(crate) fn write_report(
//     env: &ExecutionEnv,
//     sink: &FileOutput,
//     freeze_summary: Option<&FreezeSummary>,
// ) -> Result<PathBuf, CollectError> {
//     // determine version
//     let cryo_version = get_cryo_version();
//     let report = FreezeReport {
//         cryo_version,
//         cli_command: env.cli_command,
//         args: env.args,
//         results: freeze_summary,
//     };
//     let serialized = serde_json::to_string(&report)
//         .map_err(|_| CollectError::CollectError("could not serialize report".to_string()))?;

//     // create path
//     let path = get_report_path(env, sink, freeze_summary.is_some())?;

//     // save to file
//     let mut file = File::create(&path)
//         .map_err(|_| CollectError::CollectError("could not create report file".to_string()))?;
//     file.write_all(serialized.as_bytes())
//         .map_err(|_| CollectError::CollectError("could not write report data".to_string()))?;

//     Ok(path)
// }

// fn get_cryo_version() -> String {
//     format!(
//         "{}__{}",
//         env!("CARGO_PKG_VERSION"),
//         option_env!("GIT_DESCRIPTION").unwrap_or("unknown")
//     )
// }
