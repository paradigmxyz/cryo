use super::{balance_diffs, code_diffs, nonce_diffs, storage_diffs};
use crate::{
    CollectByBlock, CollectByTransaction, CollectError, ColumnData, Datatype, RpcParams, StateDiffs,
};
use std::collections::HashMap;

use crate::{Source, Table};
use polars::prelude::*;

#[async_trait::async_trait]
impl CollectByBlock for StateDiffs {
    type BlockResponse = (Option<u32>, Option<Vec<u8>>, Vec<ethers::types::BlockTrace>);

    type BlockColumns = StateDiffColumns;

    async fn extract_by_block(
        request: RpcParams,
        source: Source,
        _schemas: HashMap<Datatype, Table>,
    ) -> Result<Self::BlockResponse, CollectError> {
        source.fetcher.trace_block_state_diffs(request.block_number() as u32).await
    }

    fn transform_by_block(
        response: Self::BlockResponse,
        columns: &mut Self::BlockColumns,
        schemas: &HashMap<Datatype, Table>,
    ) {
        process_state_diffs(response, columns, schemas)
    }
}

#[async_trait::async_trait]
impl CollectByTransaction for StateDiffs {
    type TransactionResponse = (Option<u32>, Option<Vec<u8>>, Vec<ethers::types::BlockTrace>);

    type TransactionColumns = StateDiffColumns;

    async fn extract_by_transaction(
        request: RpcParams,
        source: Source,
        _schemas: HashMap<Datatype, Table>,
    ) -> Result<Self::TransactionResponse, CollectError> {
        source.fetcher.trace_block_state_diffs(request.block_number() as u32).await
    }

    fn transform_by_transaction(
        response: Self::TransactionResponse,
        columns: &mut Self::TransactionColumns,
        schemas: &HashMap<Datatype, Table>,
    ) {
        process_state_diffs(response, columns, schemas)
    }
}

fn process_state_diffs(
    response: (Option<u32>, Option<Vec<u8>>, Vec<ethers::types::BlockTrace>),
    columns: &mut StateDiffColumns,
    schemas: &HashMap<Datatype, Table>,
) {
    let StateDiffColumns(balance_columns, code_columns, nonce_columns, storage_columns) = columns;
    if let Some(schema) = schemas.get(&Datatype::BalanceDiffs) {
        balance_diffs::process_balance_diffs(&response, balance_columns, schema)
    }
    if let Some(schema) = schemas.get(&Datatype::CodeDiffs) {
        code_diffs::process_code_diffs(&response, code_columns, schema)
    }
    if let Some(schema) = schemas.get(&Datatype::NonceDiffs) {
        nonce_diffs::process_nonce_diffs(&response, nonce_columns, schema)
    }
    if let Some(schema) = schemas.get(&Datatype::StorageDiffs) {
        storage_diffs::process_storage_diffs(&response, storage_columns, schema)
    }
}

/// StateDiffColumns
#[derive(Default)]
pub struct StateDiffColumns(
    balance_diffs::BalanceDiffColumns,
    code_diffs::CodeDiffColumns,
    nonce_diffs::NonceDiffColumns,
    storage_diffs::StorageDiffColumns,
);

impl ColumnData for StateDiffColumns {
    fn datatypes() -> Vec<Datatype> {
        vec![
            Datatype::BalanceDiffs,
            Datatype::CodeDiffs,
            Datatype::NonceDiffs,
            Datatype::StorageDiffs,
        ]
    }

    fn create_dfs(
        self,
        schemas: &HashMap<Datatype, Table>,
        chain_id: u64,
    ) -> Result<HashMap<Datatype, DataFrame>, CollectError> {
        let StateDiffColumns(balance_columns, code_columns, nonce_columns, storage_columns) = self;
        Ok(vec![
            (Datatype::BalanceDiffs, balance_columns.create_df(schemas, chain_id)?),
            (Datatype::CodeDiffs, code_columns.create_df(schemas, chain_id)?),
            (Datatype::NonceDiffs, nonce_columns.create_df(schemas, chain_id)?),
            (Datatype::StorageDiffs, storage_columns.create_df(schemas, chain_id)?),
        ]
        .into_iter()
        .collect())
    }
}
