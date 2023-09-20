/// CollectByBlock
use crate::freeze2::{fetch_partition, ChunkDim, ColumnData, MetaChunk, RpcParams};
use crate::{CollectError, Source, Table};
use polars::prelude::*;
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
        schema: Table,
    ) -> Result<Self::BlockResponse, CollectError>;

    /// transform block data response into column data
    fn transform_by_block(
        response: Self::BlockResponse,
        columns: &mut Self::BlockColumns,
        schema: &Table,
    );
}

/// collect data into DataFrame
pub(crate) async fn collect_by_block<T: CollectByBlock>(
    meta_chunk: MetaChunk,
    source: Source,
    schema: Table,
) -> Result<DataFrame, CollectError> {
    let (sender, receiver) = mpsc::channel(1);
    let chain_id = source.chain_id;
    fetch_partition(
        T::extract_by_block,
        meta_chunk,
        source,
        schema.clone(),
        T::block_parameters(),
        sender,
    )
    .await?;
    block_data_to_df::<T>(receiver, schema, chain_id).await
}

/// convert block-derived data to dataframe
pub(crate) async fn block_data_to_df<T: CollectByBlock>(
    mut receiver: mpsc::Receiver<Result<T::BlockResponse, CollectError>>,
    schema: Table,
    chain_id: u64,
) -> Result<DataFrame, CollectError> {
    let mut columns = T::BlockColumns::default();
    while let Some(message) = receiver.recv().await {
        match message {
            Ok(message) => T::transform_by_block(message, &mut columns, &schema),
            Err(e) => return Err(e),
        }
    }
    columns.create_df(&schema, chain_id)
}

