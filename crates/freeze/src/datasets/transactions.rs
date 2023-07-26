use std::collections::HashMap;

use polars::prelude::*;

use super::{blocks, blocks_and_transactions};
use crate::types::{
    BlockChunk, CollectError, ColumnType, Dataset, Datatype, RowFilter, Source, Table, Transactions,
};

#[async_trait::async_trait]
impl Dataset for Transactions {
    fn datatype(&self) -> Datatype {
        Datatype::Transactions
    }

    fn name(&self) -> &'static str {
        "transactions"
    }

    fn column_types(&self) -> HashMap<&'static str, ColumnType> {
        HashMap::from_iter(vec![
            ("block_number", ColumnType::Int32),
            ("transaction_index", ColumnType::Int32),
            ("transaction_hash", ColumnType::Binary),
            ("nonce", ColumnType::Int32),
            ("from_address", ColumnType::Binary),
            ("to_address", ColumnType::Binary),
            ("value", ColumnType::Decimal128),
            ("value_str", ColumnType::String),
            ("value_float", ColumnType::Float64),
            ("input", ColumnType::Binary),
            ("gas_limit", ColumnType::UInt32),
            ("gas_price", ColumnType::UInt64),
            ("transaction_type", ColumnType::UInt32),
            ("max_priority_fee_per_gas", ColumnType::UInt64),
            ("max_fee_per_gas", ColumnType::UInt64),
            ("chain_id", ColumnType::UInt64),
        ])
    }

    fn default_columns(&self) -> Vec<&'static str> {
        vec![
            "block_number",
            "transaction_index",
            "transaction_hash",
            "nonce",
            "from_address",
            "to_address",
            "value",
            "input",
            "gas_limit",
            "gas_price",
            "transaction_type",
            "max_priority_fee_per_gas",
            "max_fee_per_gas",
            "chain_id",
        ]
    }

    fn default_sort(&self) -> Vec<String> {
        vec!["block_number".to_string(), "transaction_index".to_string()]
    }

    async fn collect_block_chunk(
        &self,
        chunk: &BlockChunk,
        source: &Source,
        schema: &Table,
        _filter: Option<&RowFilter>,
    ) -> Result<DataFrame, CollectError> {
        let rx = blocks_and_transactions::fetch_blocks_and_transactions(chunk, source).await;
        let output = blocks::blocks_to_dfs(rx, &None, &Some(schema), source.chain_id).await;
        match output {
            Ok((_, Some(txs_df))) => Ok(txs_df),
            Ok((_, _)) => Err(CollectError::BadSchemaError),
            Err(e) => Err(e),
        }
    }
}
