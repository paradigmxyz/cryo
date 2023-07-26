use std::sync::Arc;

use cryo_freeze::{FileOutput, MultiQuery, ParseError, Source};

use crate::args::Args;

use super::{file_output, query, source};

/// parse options for running freeze
pub async fn parse_opts(args: &Args) -> Result<(MultiQuery, Source, FileOutput), ParseError> {
    let source = source::parse_source(args).await?;
    let query = query::parse_query(args, Arc::clone(&source.provider)).await?;
    let sink = file_output::parse_file_output(args, &source)?;
    Ok((query, source, sink))
}
