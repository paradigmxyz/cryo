use super::{blocks, transactions};
use crate::{
    StateDiffs, ChunkDim, CollectByBlock, CollectError, ColumnData, Datatype, RpcParams,
    Transactions,
};
use std::collections::HashMap;

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
        schemas: HashMap<Datatype, Table>,
    ) -> Result<Self::BlockResponse, CollectError> {
        Transactions::extract_by_block(request, source, schemas).await
    }

    /// transform block data response into column data
    fn transform_by_block(
        response: Self::BlockResponse,
        columns: &mut Self::BlockColumns,
        schemas: &HashMap<Datatype, Table>,
    ) {
        let BlocksAndTransactionsColumns(block_columns, transaction_columns) = columns;
        let (block, _) = response.clone();
        super::blocks::process_block(
            block,
            block_columns,
            schemas.get(&Datatype::Blocks).expect("schema undefined"),
        );
        // Blocks::transform_by_block(response, block_columns, schema);
        Transactions::transform_by_block(response, transaction_columns, schemas);
    }
}

/// Blocks and Transaction Columns
#[derive(Default)]
pub struct BlocksAndTransactionsColumns(blocks::BlockColumns, transactions::TransactionColumns);

impl ColumnData for BlocksAndTransactionsColumns {
    fn datatypes() -> Vec<Datatype> {
        vec![Datatype::Blocks, Datatype::Transactions]
    }

    fn create_dfs(
        self,
        schemas: &HashMap<Datatype, Table>,
        chain_id: u64,
    ) -> Result<HashMap<Datatype, DataFrame>, CollectError> {
        let BlocksAndTransactionsColumns(block_columns, transaction_columns) = self;
        Ok(vec![
            (Datatype::Blocks, block_columns.create_df(schemas, chain_id)?),
            (Datatype::Transactions, transaction_columns.create_df(schemas, chain_id)?),
        ]
        .into_iter()
        .collect())
    }
}
