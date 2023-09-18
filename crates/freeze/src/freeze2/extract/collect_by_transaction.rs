/// CollectByTransaction
use crate::freeze2::{fetch_partition, ChunkDim, ColumnData, MetaChunk, RpcParams};
use crate::{CollectError, Source, Table};
use polars::prelude::*;
use tokio::sync::mpsc;

/// defines how to collect dataset by block
#[async_trait::async_trait]
pub trait CollectByTransaction: 'static {
    /// type of transaction data responses
    type TransactionResponse: Send;

    /// container for a dataset partition
    type TransactionColumns: ColumnData + Send;

    /// parameters for requesting data by block
    fn transaction_parameters() -> Vec<ChunkDim>;

    /// fetch dataset data by transaction
    async fn fetch_by_transaction(
        request: RpcParams,
        source: Source,
        schema: Table,
    ) -> Result<Self::TransactionResponse, CollectError>;

    /// transform block data response into column data
    fn process_transaction_response(
        message: Self::TransactionResponse,
        columns: &mut Self::TransactionColumns,
        schema: &Table,
    );

    /// collect data into DataFrame
    async fn collect_by_transaction(
        meta_chunk: MetaChunk,
        source: Source,
        schema: Table,
    ) -> Result<DataFrame, CollectError> {
        let (sender, receiver) = mpsc::channel(1);
        let chain_id = source.chain_id;
        fetch_partition(
            Self::fetch_by_transaction,
            meta_chunk,
            source,
            schema.clone(),
            Self::transaction_parameters(),
            sender,
        )
        .await?;
        Self::transaction_data_to_df(receiver, &schema, chain_id).await
    }

    /// convert transaction-derived data to dataframe
    async fn transaction_data_to_df(
        mut receiver: mpsc::Receiver<Result<Self::TransactionResponse, CollectError>>,
        schema: &Table,
        chain_id: u64,
    ) -> Result<DataFrame, CollectError> {
        let mut columns = Self::TransactionColumns::default();
        while let Some(message) = receiver.recv().await {
            match message {
                Ok(message) => Self::process_transaction_response(message, &mut columns, schema),
                Err(e) => return Err(e),
            }
        }
        columns.create_df(schema, chain_id)
    }
}
