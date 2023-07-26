use std::sync::Arc;

use cryo_freeze::FileOutput;
use cryo_freeze::MultiQuery;
use cryo_freeze::Source;
use cryo_freeze::ParseError;

use crate::args::Args;

use super::file_output;
use super::query;
use super::source;

/// parse options for running freeze
pub async fn parse_opts(args: &Args) -> Result<(MultiQuery, Source, FileOutput), ParseError> {
    let source = source::parse_source(args).await?;
    let query = query::parse_query(args, Arc::clone(&source.provider)).await?;
    let sink = file_output::parse_file_output(args, &source)?;
    Ok((query, source, sink))
}
