use super::collect_generic::fetch_partition;
use crate::{
    ChunkDim, CollectError, ToDataFrames, Datatype, Params, Partition, Schemas, Source, Table,
};
use polars::prelude::*;
use std::collections::HashMap;
use tokio::sync::mpsc;

type Result<T> = ::core::result::Result<T, CollectError>;

/// defines how to collect dataset by block
#[async_trait::async_trait]
pub trait CollectByTransaction: 'static + Send + Default + ToDataFrames {
    /// type of transaction data responses
    type Response: Send;

    /// parameters for requesting data by block
    fn transaction_parameters() -> Vec<ChunkDim> {
        vec![ChunkDim::TransactionHash]
    }

    /// fetch dataset data by transaction
    async fn extract(request: Params, source: Source, schemas: Schemas) -> Result<Self::Response>;

    /// transform block data response into column data
    fn transform(response: Self::Response, columns: &mut Self, schemas: &Schemas);

    /// collect data into DataFrame
    async fn collect_by_transaction(
        partition: Partition,
        source: Source,
        schemas: &HashMap<Datatype, Table>,
    ) -> Result<HashMap<Datatype, DataFrame>> {
        let (sender, receiver) = mpsc::channel(1);
        let chain_id = source.chain_id;
        fetch_partition(
            Self::extract,
            partition,
            source,
            schemas.clone(),
            Self::transaction_parameters(),
            sender,
        )
        .await?;
        let columns = Self::transform_channel(receiver, schemas).await?;
        columns.create_dfs(schemas, chain_id)
    }

    /// convert transaction-derived data to dataframe
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
