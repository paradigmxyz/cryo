use crate::args::Args;
use cryo_freeze::{ExecutionEnv, ExecutionEnvBuilder, ParseError};

pub(crate) fn parse_execution_env(args: &Args, n_tasks: u64) -> Result<ExecutionEnv, ParseError> {
    let args_str =
        serde_json::to_string(args).map_err(|e| ParseError::ParseError(e.to_string()))?;
    let builder = ExecutionEnvBuilder::new()
        .dry(args.dry)
        .verbose(!args.no_verbose)
        .report(!args.no_report)
        .report_dir(args.report_dir.clone())
        .args(args_str);

    let builder = if !args.no_verbose {
        builder
            .bar(n_tasks)
            .map_err(|_| ParseError::ParseError("could not create progress bar".to_string()))?
    } else {
        builder
    };

    Ok(builder.build())
}
