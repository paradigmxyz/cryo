use std::sync::Arc;

use cryo_freeze::{ExecutionEnv, FileOutput, ParseError, Query, Source};

use crate::args::Args;

use super::{execution, file_output, query, source};

/// parse options for running freeze
pub async fn parse_opts(
    args: &Args,
) -> Result<(Query, Source, FileOutput, ExecutionEnv), ParseError> {
    let source = source::parse_source(args).await?;
    let query = query::parse_query(args, Arc::clone(&source.fetcher)).await?;
    let sink = file_output::parse_file_output(args, &source)?;
    let env = execution::parse_execution_env(args, query.n_tasks() as u64)?;
    Ok((query, source, sink, env))
}
