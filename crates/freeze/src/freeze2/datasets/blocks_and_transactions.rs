use super::{blocks, transactions};
use crate::{
    freeze2::{ChunkDim, CollectByBlock, ColumnData, RpcParams},
    BlocksAndTransactions, CollectError, Transactions,
};

use crate::{Source, Table};
use polars::prelude::*;

#[async_trait::async_trait]
impl CollectByBlock for BlocksAndTransactions {
    /// type of block data responses
    type BlockResponse = <Transactions as CollectByBlock>::BlockResponse;

    /// container for a dataset partition
    type BlockColumns = BlocksAndTransactionsColumns;

    /// parameters for requesting data by block
    fn block_parameters() -> Vec<ChunkDim> {
        Transactions::block_parameters()
    }

    /// fetch dataset data by block
    async fn extract_by_block(
        request: RpcParams,
        source: Source,
        schema: Table,
    ) -> Result<Self::BlockResponse, CollectError> {
        Transactions::extract_by_block(request, source, schema).await
    }

    /// transform block data response into column data
    fn transform_by_block(
        response: Self::BlockResponse,
        columns: &mut Self::BlockColumns,
        schema: &Table,
    ) {
        let BlocksAndTransactionsColumns(block_columns, transaction_columns) = columns;
        let (block, _) = response.clone();
        super::blocks::process_block(block, block_columns, schema);
        Transactions::transform_by_block(response, transaction_columns, schema);
    }
}

/// Blocks and Transaction Columns
#[derive(Default)]
pub struct BlocksAndTransactionsColumns(blocks::BlockColumns, transactions::TransactionColumns);

impl ColumnData for BlocksAndTransactionsColumns {
    fn create_dfs(self, schema: &Table, chain_id: u64) -> Result<Vec<DataFrame>, CollectError> {
        let BlocksAndTransactionsColumns(block_columns, transaction_columns) = self;
        Ok(vec![
            block_columns.create_df(schema, chain_id)?,
            transaction_columns.create_df(schema, chain_id)?,
        ])
    }
}
