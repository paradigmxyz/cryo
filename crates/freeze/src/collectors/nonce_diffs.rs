use crate::{
    conversions::{ToVecHex, ToVecU8},
    dataframes::SortableDataFrame,
    store, with_series, with_series_binary, with_series_u256, CollectByBlock, CollectByTransaction,
    CollectError, ColumnData, ColumnEncoding, ColumnType, Datatype, NonceDiffs, RpcParams, Source,
    Table, U256Type,
};
use ethers::prelude::*;
use polars::prelude::*;
use std::collections::HashMap;

#[async_trait::async_trait]
impl CollectByBlock for NonceDiffs {
    type BlockResponse = (Option<u32>, Option<Vec<u8>>, Vec<ethers::types::BlockTrace>);

    type BlockColumns = NonceDiffColumns;

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
        if let Some(schema) = schemas.get(&Datatype::NonceDiffs) {
            process_nonce_diffs(&response, columns, schema)
        } else {
            panic!("missing schema")
        }
    }
}

#[async_trait::async_trait]
impl CollectByTransaction for NonceDiffs {
    type TransactionResponse = (Option<u32>, Option<Vec<u8>>, Vec<ethers::types::BlockTrace>);

    type TransactionColumns = NonceDiffColumns;

    async fn extract_by_transaction(
        request: RpcParams,
        source: Source,
        _schemas: HashMap<Datatype, Table>,
    ) -> Result<Self::TransactionResponse, CollectError> {
        source.fetcher.trace_transaction_state_diffs(request.transaction_hash()).await
    }

    fn transform_by_transaction(
        response: Self::TransactionResponse,
        columns: &mut Self::TransactionColumns,
        schemas: &HashMap<Datatype, Table>,
    ) {
        if let Some(schema) = schemas.get(&Datatype::NonceDiffs) {
            process_nonce_diffs(&response, columns, schema)
        } else {
            panic!("missing schema")
        }
    }
}

/// columns for transactions
#[cryo_to_df::to_df(Datatype::NonceDiffs)]
#[derive(Default)]
pub struct NonceDiffColumns {
    n_rows: u64,
    block_number: Vec<Option<u32>>,
    transaction_index: Vec<Option<u64>>,
    transaction_hash: Vec<Option<Vec<u8>>>,
    address: Vec<Vec<u8>>,
    from_value: Vec<U256>,
    to_value: Vec<U256>,
}

pub(crate) fn process_nonce_diffs(
    response: &(Option<u32>, Option<Vec<u8>>, Vec<ethers::types::BlockTrace>),
    columns: &mut NonceDiffColumns,
    schema: &Table,
) {
    let (block_number, tx, traces) = response;
    for (index, trace) in traces.iter().enumerate() {
        if let Some(ethers::types::StateDiff(state_diffs)) = &trace.state_diff {
            for (addr, diff) in state_diffs.iter() {
                process_nonce_diff(addr, &diff.nonce, block_number, tx, index, columns, schema);
            }
        }
    }
}

pub(crate) fn process_nonce_diff(
    addr: &H160,
    diff: &Diff<U256>,
    block_number: &Option<u32>,
    transaction_hash: &Option<Vec<u8>>,
    transaction_index: usize,
    columns: &mut NonceDiffColumns,
    schema: &Table,
) {
    columns.n_rows += 1;
    let (from, to) = match diff {
        Diff::Same => (U256::zero(), U256::zero()),
        Diff::Born(value) => (U256::zero(), *value),
        Diff::Died(value) => (*value, U256::zero()),
        Diff::Changed(ChangedType { from, to }) => (*from, *to),
    };
    store!(schema, columns, block_number, *block_number);
    store!(schema, columns, transaction_index, Some(transaction_index as u64));
    store!(schema, columns, transaction_hash, transaction_hash.clone());
    store!(schema, columns, address, addr.as_bytes().to_vec());
    store!(schema, columns, from_value, from);
    store!(schema, columns, to_value, to);
}
