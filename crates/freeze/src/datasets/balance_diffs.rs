use ethers::providers::{JsonRpcClient, Provider};
use std::collections::HashMap;

use polars::prelude::*;

use super::state_diffs;
use crate::types::{
    BalanceDiffs, BlockChunk, CollectError, ColumnType, Dataset, Datatype, RowFilter, Source,
    Table, TransactionChunk,
};

#[async_trait::async_trait]
impl Dataset for BalanceDiffs {
    fn datatype(&self) -> Datatype {
        Datatype::BalanceDiffs
    }

    fn name(&self) -> &'static str {
        "balance_diffs"
    }

    fn column_types(&self) -> HashMap<&'static str, ColumnType> {
        HashMap::from_iter(vec![
            ("block_number", ColumnType::UInt32),
            ("transaction_index", ColumnType::Binary),
            ("transaction_hash", ColumnType::Binary),
            ("address", ColumnType::Binary),
            ("from_value", ColumnType::UInt256),
            ("to_value", ColumnType::UInt256),
            ("chain_id", ColumnType::UInt64),
        ])
    }

    fn default_columns(&self) -> Vec<&'static str> {
        vec![
            "block_number",
            "transaction_index",
            "transaction_hash",
            "address",
            "from_value",
            "to_value",
        ]
    }

    fn default_sort(&self) -> Vec<String> {
        vec!["block_number".to_string(), "transaction_index".to_string()]
    }

    async fn collect_block_chunk(
        &self,
        chunk: &BlockChunk,
        source: &Source<Provider<impl JsonRpcClient>>,
        schema: &Table,
        filter: Option<&RowFilter>,
    ) -> Result<DataFrame, CollectError> {
        state_diffs::collect_block_state_diffs(
            &Datatype::BalanceDiffs,
            chunk,
            source,
            schema,
            filter,
        )
        .await
    }

    async fn collect_transaction_chunk(
        &self,
        chunk: &TransactionChunk,
        source: &Source<Provider<impl JsonRpcClient>>,
        schema: &Table,
        filter: Option<&RowFilter>,
    ) -> Result<DataFrame, CollectError> {
        state_diffs::collect_transaction_state_diffs(
            &Datatype::BalanceDiffs,
            chunk,
            source,
            schema,
            filter,
        )
        .await
    }
}
