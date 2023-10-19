use crate::args::Args;
use cryo_freeze::{ExecutionEnv, ExecutionEnvBuilder, ParseError};

pub(crate) fn parse_execution_env(args: &Args, n_tasks: u64) -> Result<ExecutionEnv, ParseError> {
    let args_str =
        serde_json::to_string(args).map_err(|e| ParseError::ParseError(e.to_string()))?;

    let verbose = match (args.no_verbose, args.verbose) {
        (true, true) => return Err(ParseError::ParseError("".to_string())),
        (true, false) => 0,
        (false, true) => 2, // future: allow arbitrary numbers
        (false, false) => 1,
    };

    let builder = ExecutionEnvBuilder::new()
        .dry(args.dry)
        .verbose(verbose)
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
