use crate::args::Args;
use cryo_freeze::{ExecutionEnv, ExecutionEnvBuilder, ParseError};

pub(crate) fn parse_execution_env(args: &Args) -> Result<ExecutionEnv, ParseError> {
    let args_str =
        serde_json::to_string(args).map_err(|e| ParseError::ParseError(e.to_string()))?;
    Ok(ExecutionEnvBuilder::new()
        .dry(args.dry)
        .verbose(!args.no_verbose)
        .report(!args.no_report)
        .report_dir(args.report_dir.clone())
        .args(args_str)
        .build())
}
