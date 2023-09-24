use std::collections::HashMap;

use ethers::providers::JsonRpcClient;
use polars::prelude::*;

use super::state_diffs;
use crate::types::{
    BlockChunk, CollectError, ColumnType, Dataset, Datatype, NonceDiffs, RowFilter, Source, Table,
    TransactionChunk,
};

#[async_trait::async_trait]
impl Dataset for NonceDiffs {
    fn datatype(&self) -> Datatype {
        Datatype::NonceDiffs
    }

    fn name(&self) -> &'static str {
        "nonce_diffs"
    }

    fn column_types(&self) -> HashMap<&'static str, ColumnType> {
        HashMap::from_iter(vec![
            ("block_number", ColumnType::UInt32),
            ("transaction_index", ColumnType::Binary),
            ("transaction_hash", ColumnType::Binary),
            ("address", ColumnType::Binary),
            ("from_value", ColumnType::Binary),
            ("to_value", ColumnType::Binary),
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

    async fn collect_block_chunk<P>(
        &self,
        chunk: &BlockChunk,
        source: &Source<P>,
        schema: &Table,
        filter: Option<&RowFilter>,
    ) -> Result<DataFrame, CollectError>
    where
        P: JsonRpcClient,
    {
        state_diffs::collect_block_state_diffs(&Datatype::NonceDiffs, chunk, source, schema, filter)
            .await
    }

    async fn collect_transaction_chunk<P>(
        &self,
        chunk: &TransactionChunk,
        source: &Source<P>,
        schema: &Table,
        filter: Option<&RowFilter>,
    ) -> Result<DataFrame, CollectError>
    where
        P: JsonRpcClient,
    {
        state_diffs::collect_transaction_state_diffs(
            &Datatype::NonceDiffs,
            chunk,
            source,
            schema,
            filter,
        )
        .await
    }
}
