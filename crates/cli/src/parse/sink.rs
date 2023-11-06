use super::file_output;
use crate::args::Args;
use cryo_freeze::{FileSink, ParseError, Sink, Source};
use std::sync::Arc;

pub(crate) fn parse_sink(args: &Args, source: &Source) -> Result<Arc<dyn Sink>, ParseError> {
    let file_output = file_output::parse_file_output(args, source)?;
    let sink = FileSink(file_output);
    Ok(Arc::new(sink))
}
