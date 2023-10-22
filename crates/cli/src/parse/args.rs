use super::{execution, query, sink, source};
use crate::args::Args;
use clap_cryo::Parser;
use cryo_freeze::{ExecutionEnv, ParseError, Query, Sink, Source};
use std::sync::Arc;

/// parse options for running freeze
pub async fn parse_args(
    args: &Args,
) -> Result<(Query, Source, Arc<dyn Sink>, ExecutionEnv), ParseError> {
    let source = source::parse_source(args).await?;
    let query = query::parse_query(args, Arc::clone(&source.fetcher)).await?;
    let env = execution::parse_execution_env(args, query.n_tasks() as u64)?;
    let sink = sink::parse_sink(args, &source)?;
    Ok((query, source, sink, env))
}

/// parse command string
#[allow(dead_code)]
pub async fn parse_str(command: &str) -> Result<Args, ParseError> {
    Ok(Args::parse_from(command.split_whitespace()))
}
