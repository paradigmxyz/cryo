use std::collections::HashMap;

use polars::prelude::*;

use crate::types::{CollectError, Datatype, MultiQuery, SingleQuery, Source};

/// collect data and return as dataframe
pub async fn collect(query: SingleQuery, source: Source) -> Result<DataFrame, CollectError> {
    if query.chunks.len() > 1 {
        return Err(CollectError::CollectError("can only collect 1 chunk".to_string()))
    };
    let chunk = match query.chunks.first() {
        Some((chunk, _)) => chunk,
        _ => return Err(CollectError::CollectError("no chunks".to_string())),
    };
    let filter = query.row_filter.as_ref();
    query.datatype.dataset().collect_chunk(chunk, &source, &query.schema, filter).await
}

/// collect data and return as dataframe
pub async fn collect_multiple(
    _query: MultiQuery,
    _source: Source,
) -> Result<HashMap<Datatype, DataFrame>, CollectError> {
    todo!()
}
