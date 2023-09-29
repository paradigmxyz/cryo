use super::collect_generic::fetch_partition;
use crate::{ChunkDim, CollectError, ColumnData, Datatype, Partition, RpcParams, Source, Table};
use polars::prelude::*;
use std::collections::HashMap;
use tokio::sync::mpsc;

/// defines how to collect dataset by block
#[async_trait::async_trait]
pub trait CollectByBlock: 'static + Send {
    /// type of block data responses
    type BlockResponse: Send;

    /// container for a dataset partition
    type BlockColumns: ColumnData + Send;

    /// parameters for requesting data by block
    fn block_parameters() -> Vec<ChunkDim>;

    /// fetch dataset data by block
    async fn extract_by_block(
        request: RpcParams,
        source: Source,
        schemas: HashMap<Datatype, Table>,
    ) -> Result<Self::BlockResponse, CollectError>;

    /// transform block data response into column data
    fn transform_by_block(
        response: Self::BlockResponse,
        columns: &mut Self::BlockColumns,
        schemas: &HashMap<Datatype, Table>,
    );

    /// collect data into DataFrame
    async fn collect_by_block(
        partition: Partition,
        source: Source,
        schemas: &HashMap<Datatype, Table>,
    ) -> Result<HashMap<Datatype, DataFrame>, CollectError> {
        let (sender, receiver) = mpsc::channel(1);
        let chain_id = source.chain_id;
        fetch_partition(
            Self::extract_by_block,
            partition,
            source,
            schemas.clone(),
            Self::block_parameters(),
            sender,
        )
        .await?;
        Self::block_data_to_dfs(receiver, schemas, chain_id).await
    }

    /// convert block-derived data to dataframe
    async fn block_data_to_dfs(
        mut receiver: mpsc::Receiver<Result<Self::BlockResponse, CollectError>>,
        schemas: &HashMap<Datatype, Table>,
        chain_id: u64,
    ) -> Result<HashMap<Datatype, DataFrame>, CollectError> {
        let mut columns = Self::BlockColumns::default();
        while let Some(message) = receiver.recv().await {
            match message {
                Ok(message) => Self::transform_by_block(message, &mut columns, schemas),
                Err(e) => return Err(e),
            }
        }
        columns.create_dfs(schemas, chain_id)
    }

    // /// singular version of collect_by_block()
    // async fn collect_single_by_block(
    //     partition: Partition,
    //     source: Source,
    //     schemas: HashMap<Datatype, Table>,
    // ) -> Result<DataFrame, CollectError> { let dfs = Self::collect_by_block(partition, source,
    //   schemas).await?; if dfs.len() == 1 { for (_datatype, df) in dfs.drain().next() { return
    //   Ok(df) } } Err(CollectError::CollectError("improper number of dataframes
    //   returned".to_string()))
    // }

    // /// singular version of block_data_to_dfs()
    // async fn block_data_to_df(
    //     mut receiver: mpsc::Receiver<Result<Self::BlockResponse, CollectError>>,
    //     schemas: HashMap<Datatype, Table>,
    //     chain_id: u64,
    // ) -> Result<DataFrame, CollectError> { let dfs = Self::block_data_to_dfs(receiver, schemas,
    //   chain_id).await?; if dfs.len() == 1 { for (datatype, df) in dfs.drain().next() { return
    //   Ok(df) } } Err(CollectError::CollectError("improper number of dataframes
    //   returned".to_string()))
    // }
}
