use std::collections::HashMap;

use polars::prelude::*;

use crate::types::CollectError;
use crate::types::Datatype;
use crate::types::MultiQuery;
use crate::types::SingleQuery;
use crate::types::Source;

/// collect data and return as dataframe
pub async fn collect(_query: SingleQuery, _source: Source) -> Result<DataFrame, CollectError> {
    todo!()
}

/// collect data and return as dataframe
pub async fn collect_multiple(
    _query: MultiQuery,
    _source: Source,
) -> Result<HashMap<Datatype, DataFrame>, CollectError> {
    todo!()
}
