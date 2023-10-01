use crate::*;
use polars::prelude::*;
use std::collections::{HashMap, HashSet};

/// Blocks and Transaction Columns
#[derive(Default)]
pub struct BlocksAndTransactionsColumns(
    <Blocks as CollectByBlock>::Columns,
    <Transactions as CollectByBlock>::Columns,
);

#[async_trait::async_trait]
impl MultiDataset for BlocksAndTransactions {
    fn name(&self) -> &'static str {
        "blocks_and_transactions"
    }

    fn datatypes(&self) -> HashSet<Datatype> {
        [Datatype::Blocks, Datatype::Transactions].into_iter().collect()
    }
}

type Result<T> = ::core::result::Result<T, CollectError>;

#[async_trait::async_trait]
impl CollectByBlock for BlocksAndTransactions {
    /// type of block data responses
    type Response = <Transactions as CollectByBlock>::Response;

    /// container for a dataset partition
    type Columns = BlocksAndTransactionsColumns;

    /// parameters for requesting data by block
    fn block_parameters() -> Vec<ChunkDim> {
        Transactions::block_parameters()
    }

    /// fetch dataset data by block
    async fn extract(request: Params, source: Source, schemas: Schemas) -> Result<Self::Response> {
        <Transactions as CollectByBlock>::extract(request, source, schemas).await
    }

    /// transform block data response into column data
    fn transform(response: Self::Response, columns: &mut Self::Columns, schemas: &Schemas) {
        let BlocksAndTransactionsColumns(block_columns, transaction_columns) = columns;
        let (block, _) = response.clone();
        super::blocks::process_block(
            block,
            block_columns,
            schemas.get(&Datatype::Blocks).expect("schema undefined"),
        );
        <Transactions as CollectByBlock>::transform(response, transaction_columns, schemas);
    }
}
impl ColumnData for BlocksAndTransactionsColumns {
    fn datatypes() -> Vec<Datatype> {
        vec![Datatype::Blocks, Datatype::Transactions]
    }

    fn create_dfs(self, schemas: &Schemas, chain_id: u64) -> Result<HashMap<Datatype, DataFrame>> {
        let BlocksAndTransactionsColumns(block_columns, transaction_columns) = self;
        Ok(vec![
            (Datatype::Blocks, block_columns.create_df(schemas, chain_id)?),
            (Datatype::Transactions, transaction_columns.create_df(schemas, chain_id)?),
        ]
        .into_iter()
        .collect())
    }
}
