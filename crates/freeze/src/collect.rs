use std::collections::HashMap;

use polars::prelude::*;

use crate::types::{Chunk, CollectError, Datatype, MultiQuery, SingleQuery, Source};

/// collect data and return as dataframe
pub async fn collect(query: SingleQuery, source: Source) -> Result<DataFrame, CollectError> {
    let chunk: Chunk = query.chunks.into();
    let filter = query.row_filter.as_ref();
    query.datatype.dataset().collect_chunk(&chunk, &source, &query.schema, filter).await
}

/// collect data and return as dataframe
pub async fn collect_multiple(
    _query: MultiQuery,
    _source: Source,
) -> Result<HashMap<Datatype, DataFrame>, CollectError> {
    todo!()
}
