use crate::{collect_partition, CollectError, Query, Source};
use polars::prelude::*;

/// collect single dataframe
pub async fn collect(query: Query, source: Source) -> Result<DataFrame, CollectError> {
    query.is_valid()?;
    let datatype = if query.datatypes.len() != 1 { panic!() } else { query.datatypes[0].clone() };
    let partition =
        if query.partitions.len() != 1 { panic!() } else { query.partitions[0].clone() };
    let results =
        collect_partition(query.time_dimension, datatype, partition, source, query.schemas).await?;
    if results.len() > 1 {
        panic!("collect() only returns single dataframes")
    } else {
        Ok(results.into_iter().next().map(|(_, v)| v).unwrap())
    }
}
