use super::{parse_schemas, partitions};
use crate::args::Args;
use cryo_freeze::{Fetcher, ParseError, Query};
use ethers::prelude::*;
use std::sync::Arc;

pub(crate) async fn parse_query<P: JsonRpcClient>(
    args: &Args,
    fetcher: Arc<Fetcher<P>>,
) -> Result<Query, ParseError> {
    let schemas = parse_schemas(args)?;
    let (partitions, partitioned_by, time_dimension) =
        partitions::parse_partitions(args, fetcher, &schemas).await?;
    let datatypes = cryo_freeze::cluster_datatypes(schemas.keys().cloned().collect());
    Ok(Query { datatypes, schemas, time_dimension, partitions, partitioned_by })
}
