/// CollectByTransaction
use crate::freeze2::{fetch_partition, ChunkDim, ColumnData, MetaChunk, RpcParams};
use crate::{CollectError, Source, Table};
use polars::prelude::*;
use tokio::sync::mpsc;

/// defines how to collect dataset by block
#[async_trait::async_trait]
pub trait CollectByTransaction: 'static + Send {
    /// type of transaction data responses
    type TransactionResponse: Send;

    /// container for a dataset partition
    type TransactionColumns: ColumnData + Send;

    /// parameters for requesting data by block
    fn transaction_parameters() -> Vec<ChunkDim>;

    /// fetch dataset data by transaction
    async fn extract_by_transaction(
        request: RpcParams,
        source: Source,
        schema: Table,
    ) -> Result<Self::TransactionResponse, CollectError>;

    /// transform block data response into column data
    fn transform_by_transaction(
        response: Self::TransactionResponse,
        columns: &mut Self::TransactionColumns,
        schema: &Table,
    );
}

/// collect data into DataFrame
pub(crate) async fn collect_by_transaction<T: CollectByTransaction>(
    meta_chunk: MetaChunk,
    source: Source,
    schema: Table,
) -> Result<DataFrame, CollectError> {
    let (sender, receiver) = mpsc::channel(1);
    let chain_id = source.chain_id;
    fetch_partition(
        T::extract_by_transaction,
        meta_chunk,
        source,
        schema.clone(),
        T::transaction_parameters(),
        sender,
    )
    .await?;
    transaction_data_to_df::<T>(receiver, &schema, chain_id).await
}

/// convert transaction-derived data to dataframe
pub(crate) async fn transaction_data_to_df<T: CollectByTransaction>(
    mut receiver: mpsc::Receiver<Result<T::TransactionResponse, CollectError>>,
    schema: &Table,
    chain_id: u64,
) -> Result<DataFrame, CollectError> {
    let mut columns = T::TransactionColumns::default();
    while let Some(message) = receiver.recv().await {
        match message {
            Ok(message) => T::transform_by_transaction(message, &mut columns, schema),
            Err(e) => return Err(e),
        }
    }
    columns.create_df(schema, chain_id)
}
