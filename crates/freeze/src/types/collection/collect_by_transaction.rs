use super::collect_generic::fetch_partition;
use crate::{ChunkDim, CollectError, ColumnData, Datatype, Partition, RpcParams, Source, Table};
use polars::prelude::*;
use std::collections::HashMap;
use tokio::sync::mpsc;

/// defines how to collect dataset by block
#[async_trait::async_trait]
pub trait CollectByTransaction: 'static + Send {
    /// type of transaction data responses
    type TransactionResponse: Send;

    /// container for a dataset partition
    type TransactionColumns: ColumnData + Send;

    /// parameters for requesting data by block
    fn transaction_parameters() -> Vec<ChunkDim> {
        vec![ChunkDim::TransactionHash]
    }

    /// fetch dataset data by transaction
    async fn extract_by_transaction(
        request: RpcParams,
        source: Source,
        schemas: HashMap<Datatype, Table>,
    ) -> Result<Self::TransactionResponse, CollectError>;

    /// transform block data response into column data
    fn transform_by_transaction(
        response: Self::TransactionResponse,
        columns: &mut Self::TransactionColumns,
        schemas: &HashMap<Datatype, Table>,
    );

    /// collect data into DataFrame
    async fn collect_by_transaction(
        partition: Partition,
        source: Source,
        schemas: &HashMap<Datatype, Table>,
    ) -> Result<HashMap<Datatype, DataFrame>, CollectError> {
        let (sender, receiver) = mpsc::channel(1);
        let chain_id = source.chain_id;
        fetch_partition(
            Self::extract_by_transaction,
            partition,
            source,
            schemas.clone(),
            Self::transaction_parameters(),
            sender,
        )
        .await?;
        Self::transaction_data_to_dfs(receiver, schemas, chain_id).await
    }

    /// convert transaction-derived data to dataframe
    async fn transaction_data_to_dfs(
        mut receiver: mpsc::Receiver<Result<Self::TransactionResponse, CollectError>>,
        schemas: &HashMap<Datatype, Table>,
        chain_id: u64,
    ) -> Result<HashMap<Datatype, DataFrame>, CollectError> {
        let mut columns = Self::TransactionColumns::default();
        while let Some(message) = receiver.recv().await {
            match message {
                Ok(message) => Self::transform_by_transaction(message, &mut columns, schemas),
                Err(e) => return Err(e),
            }
        }
        columns.create_dfs(schemas, chain_id)
    }
}
