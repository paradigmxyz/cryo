use super::collect_generic::fetch_partition;
use crate::{CollectError, Datatype, Params, Partition, Schemas, Source, Table, ToDataFrames};
use polars::prelude::*;
use std::collections::HashMap;
use tokio::sync::mpsc;

type Result<T> = ::core::result::Result<T, CollectError>;

/// defines how to collect dataset by block
#[async_trait::async_trait]
pub trait CollectByBlock: 'static + Send + Default + ToDataFrames {
    /// type of block data responses
    type Response: Send;

    /// fetch dataset data by block
    async fn extract(_request: Params, _: Source, _: Schemas) -> Result<Self::Response> {
        Err(CollectError::CollectError("CollectByBlock not implemented".to_string()))
    }

    /// transform block data response into column data
    fn transform(_response: Self::Response, _columns: &mut Self, _schemas: &Schemas) {
        panic!("CollectByBlock not implemented")
    }

    /// collect data into DataFrame
    async fn collect_by_block(
        partition: Partition,
        source: Source,
        schemas: &HashMap<Datatype, Table>,
    ) -> Result<HashMap<Datatype, DataFrame>> {
        let (sender, receiver) = mpsc::channel(1);
        let chain_id = source.chain_id;
        fetch_partition(Self::extract, partition, source, schemas.clone(), sender).await?;
        let columns = Self::transform_channel(receiver, schemas).await?;
        columns.create_dfs(schemas, chain_id)
    }

    /// convert block-derived data to dataframe
    async fn transform_channel(
        mut receiver: mpsc::Receiver<Result<Self::Response>>,
        schemas: &HashMap<Datatype, Table>,
    ) -> Result<Self> {
        let mut columns = Self::default();
        while let Some(message) = receiver.recv().await {
            match message {
                Ok(message) => Self::transform(message, &mut columns, schemas),
                Err(e) => return Err(e),
            }
        }
        Ok(columns)
    }
}
