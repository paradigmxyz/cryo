use crate::{args, parse};
use cryo_freeze::{CollectError, ExecutionEnv, FreezeSummary};
use std::{sync::Arc, time::SystemTime};

/// run cli
pub async fn run(args: args::Args) -> Result<Option<FreezeSummary>, CollectError> {
    let t_start_parse = Some(SystemTime::now());
    let (query, source, sink, env) = match parse::parse_args(&args).await {
        Ok(opts) => opts,
        Err(e) => return Err(e.into()),
    };
    let source = Arc::new(source);
    let env = ExecutionEnv { t_start_parse, ..env };
    let env = env.set_start_time();
    cryo_freeze::freeze(&query, &source, &sink, &env).await
}
