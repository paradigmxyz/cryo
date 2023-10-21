use crate::{collect_partition, CollectError, Query, Source};
use polars::prelude::*;

/// collect single dataframe
pub async fn collect(query: Arc<Query>, source: Arc<Source>) -> Result<DataFrame, CollectError> {
    query.is_valid()?;
    let datatype = if query.datatypes.len() != 1 {
        return Err(CollectError::CollectError(
            "collect() can only collect a single datatype".to_string(),
        ))
    } else {
        query.datatypes[0].clone()
    };
    let partition = if query.partitions.len() != 1 {
        return Err(CollectError::CollectError(
            "collect() can only collect a single datatype".to_string(),
        ))
    } else {
        query.partitions[0].clone()
    };
    let results = collect_partition(datatype, partition, query, source).await?;
    if results.len() > 1 {
        Err(CollectError::CollectError("collect() only returns single dataframes".to_string()))
    } else {
        match results.into_iter().next() {
            Some((_datatype, df)) => Ok(df),
            None => Err(CollectError::CollectError("no dataframe result returned".to_string())),
        }
    }
}
