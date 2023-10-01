use super::{balance_diffs, code_diffs, nonce_diffs, storage_diffs};
use crate::*;
use polars::prelude::*;
use std::collections::{HashMap, HashSet};

/// StateDiffColumns
#[derive(Default)]
pub struct StateDiffColumns(
    balance_diffs::BalanceDiffColumns,
    code_diffs::CodeDiffColumns,
    nonce_diffs::NonceDiffColumns,
    storage_diffs::StorageDiffColumns,
);

type Result<T> = ::core::result::Result<T, CollectError>;

// entries: block_number, transaction_indices, transaction_traces
// pub(crate) type BlockNumberTransactionsTraces = Result<(Option<u32>, Vec<(u32, BlockTrace)>)>;

#[async_trait::async_trait]
impl MultiDataset for StateDiffs {
    fn name(&self) -> &'static str {
        "state_diffs"
    }

    fn datatypes(&self) -> HashSet<Datatype> {
        [Datatype::BalanceDiffs, Datatype::CodeDiffs, Datatype::NonceDiffs, Datatype::StorageDiffs]
            .into_iter()
            .collect()
    }
}

#[async_trait::async_trait]
impl CollectByBlock for StateDiffs {
    type Response = (Option<u32>, Option<Vec<u8>>, Vec<ethers::types::BlockTrace>);

    type Columns = StateDiffColumns;

    async fn extract(request: Params, source: Source, _schemas: Schemas) -> Result<Self::Response> {
        source.fetcher.trace_block_state_diffs(request.block_number() as u32).await
    }

    fn transform(response: Self::Response, columns: &mut Self::Columns, schemas: &Schemas) {
        process_state_diffs(response, columns, schemas)
    }
}

#[async_trait::async_trait]
impl CollectByTransaction for StateDiffs {
    type Response = (Option<u32>, Option<Vec<u8>>, Vec<ethers::types::BlockTrace>);

    type Columns = StateDiffColumns;

    async fn extract(request: Params, source: Source, _schemas: Schemas) -> Result<Self::Response> {
        source.fetcher.trace_transaction_state_diffs(request.transaction_hash()).await
    }

    fn transform(response: Self::Response, columns: &mut Self::Columns, schemas: &Schemas) {
        process_state_diffs(response, columns, schemas)
    }
}

fn process_state_diffs(
    response: (Option<u32>, Option<Vec<u8>>, Vec<ethers::types::BlockTrace>),
    columns: &mut StateDiffColumns,
    schemas: &HashMap<Datatype, Table>,
) {
    let StateDiffColumns(balance_columns, code_columns, nonce_columns, storage_columns) = columns;
    if let Some(_schema) = schemas.get(&Datatype::BalanceDiffs) {
        balance_diffs::process_balance_diffs(&response, balance_columns, schemas)
    }
    if let Some(_schema) = schemas.get(&Datatype::CodeDiffs) {
        code_diffs::process_code_diffs(&response, code_columns, schemas)
    }
    if let Some(_schema) = schemas.get(&Datatype::NonceDiffs) {
        nonce_diffs::process_nonce_diffs(&response, nonce_columns, schemas)
    }
    if let Some(_schema) = schemas.get(&Datatype::StorageDiffs) {
        storage_diffs::process_storage_diffs(&response, storage_columns, schemas)
    }
}

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
    ) -> Result<HashMap<Datatype, DataFrame>> {
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
