use crate::{
    conversions::ToVecHex, dataframes::SortableDataFrame, store, with_series, with_series_binary,
    CodeDiffs, CollectByBlock, CollectByTransaction, CollectError, ColumnData, ColumnType, Dataset,
    Datatype, Params, Schemas, Source, Table,
};
use ethers::prelude::*;
use polars::prelude::*;
use std::collections::HashMap;

/// columns for transactions
#[cryo_to_df::to_df(Datatype::CodeDiffs)]
#[derive(Default)]
pub struct CodeDiffColumns {
    n_rows: u64,
    block_number: Vec<Option<u32>>,
    transaction_index: Vec<Option<u64>>,
    transaction_hash: Vec<Option<Vec<u8>>>,
    address: Vec<Vec<u8>>,
    from_value: Vec<Vec<u8>>,
    to_value: Vec<Vec<u8>>,
}

#[async_trait::async_trait]
impl Dataset for CodeDiffs {
    fn datatype(&self) -> Datatype {
        Datatype::CodeDiffs
    }

    fn name(&self) -> &'static str {
        "code_diffs"
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
}

type Result<T> = ::core::result::Result<T, CollectError>;

#[async_trait::async_trait]
impl CollectByBlock for CodeDiffs {
    type Response = (Option<u32>, Option<Vec<u8>>, Vec<ethers::types::BlockTrace>);

    type Columns = CodeDiffColumns;

    async fn extract(request: Params, source: Source, _schemas: Schemas) -> Result<Self::Response> {
        source.fetcher.trace_block_state_diffs(request.block_number() as u32).await
    }

    fn transform(response: Self::Response, columns: &mut Self::Columns, schemas: &Schemas) {
        process_code_diffs(&response, columns, schemas)
    }
}

#[async_trait::async_trait]
impl CollectByTransaction for CodeDiffs {
    type Response = (Option<u32>, Option<Vec<u8>>, Vec<ethers::types::BlockTrace>);

    type Columns = CodeDiffColumns;

    async fn extract(request: Params, source: Source, _schemas: Schemas) -> Result<Self::Response> {
        source.fetcher.trace_transaction_state_diffs(request.transaction_hash()).await
    }

    fn transform(response: Self::Response, columns: &mut Self::Columns, schemas: &Schemas) {
        process_code_diffs(&response, columns, schemas)
    }
}

pub(crate) fn process_code_diffs(
    response: &(Option<u32>, Option<Vec<u8>>, Vec<ethers::types::BlockTrace>),
    columns: &mut CodeDiffColumns,
    schemas: &Schemas,
) {
    let schema = schemas.get(&Datatype::CodeDiffs).expect("missing schema");
    let (block_number, tx, traces) = response;
    for (index, trace) in traces.iter().enumerate() {
        if let Some(ethers::types::StateDiff(state_diffs)) = &trace.state_diff {
            for (addr, diff) in state_diffs.iter() {
                process_code_diff(addr, &diff.code, block_number, tx, index, columns, schema);
            }
        }
    }
}

pub(crate) fn process_code_diff(
    addr: &H160,
    diff: &Diff<Bytes>,
    block_number: &Option<u32>,
    transaction_hash: &Option<Vec<u8>>,
    transaction_index: usize,
    columns: &mut CodeDiffColumns,
    schema: &Table,
) {
    columns.n_rows += 1;
    let (from, to) = match diff {
        Diff::Same => (H256::zero().as_bytes().to_vec(), H256::zero().as_bytes().to_vec()),
        Diff::Born(value) => (H256::zero().as_bytes().to_vec(), value.to_vec()),
        Diff::Died(value) => (value.to_vec(), H256::zero().as_bytes().to_vec()),
        Diff::Changed(ChangedType { from, to }) => (from.to_vec(), to.to_vec()),
    };
    store!(schema, columns, block_number, *block_number);
    store!(schema, columns, transaction_index, Some(transaction_index as u64));
    store!(schema, columns, transaction_hash, transaction_hash.clone());
    store!(schema, columns, address, addr.as_bytes().to_vec());
    store!(schema, columns, from_value, from);
    store!(schema, columns, to_value, to);
}
