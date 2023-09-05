use std::collections::HashMap;

use ethers::prelude::*;
use polars::prelude::*;
use tokio::sync::mpsc;

use super::traces;
use crate::{
    types::{
        conversions::ToVecHex, dataframes::SortableDataFrame, BlockChunk, CollectError, ColumnType,
        Dataset, Datatype, NativeTransfers, RowFilter, Source, Table, ToVecU8, TransactionChunk,
    },
    with_series, with_series_binary,
};

#[async_trait::async_trait]
impl Dataset for NativeTransfers {
    fn datatype(&self) -> Datatype {
        Datatype::NativeTransfers
    }

    fn name(&self) -> &'static str {
        "native_transfers"
    }

    fn column_types(&self) -> HashMap<&'static str, ColumnType> {
        HashMap::from_iter(vec![
            ("block_number", ColumnType::UInt32),
            ("transaction_index", ColumnType::UInt32),
            ("transfer_index", ColumnType::UInt32),
            ("transaction_hash", ColumnType::Binary),
            ("from_address", ColumnType::Binary),
            ("to_address", ColumnType::Binary),
            ("value", ColumnType::UInt256),
            ("chain_id", ColumnType::UInt64),
        ])
    }

    fn default_columns(&self) -> Vec<&'static str> {
        vec![
            "block_number",
            "transaction_index",
            "transfer_index",
            "transaction_hash",
            "from_address",
            "to_address",
            "value",
        ]
    }

    fn default_sort(&self) -> Vec<String> {
        vec!["block_number".to_string(), "transfer_index".to_string()]
    }

    async fn collect_block_chunk(
        &self,
        chunk: &BlockChunk,
        source: &Source,
        schema: &Table,
        _filter: Option<&RowFilter>,
    ) -> Result<DataFrame, CollectError> {
        let rx = traces::fetch_block_traces(chunk, source);
        traces_to_native_transfers_df(rx, schema, source.chain_id).await
    }

    async fn collect_transaction_chunk(
        &self,
        chunk: &TransactionChunk,
        source: &Source,
        schema: &Table,
        _filter: Option<&RowFilter>,
    ) -> Result<DataFrame, CollectError> {
        let rx = traces::fetch_transaction_traces(chunk, source);
        traces_to_native_transfers_df(rx, schema, source.chain_id).await
    }
}

struct NativeTransfersColumns {
    block_number: Vec<u64>,
    transaction_index: Vec<Option<u32>>,
    transfer_index: Vec<u32>,
    transaction_hash: Vec<Option<Vec<u8>>>,
    from_address: Vec<Vec<u8>>,
    to_address: Vec<Vec<u8>>,
    value: Vec<Vec<u8>>,
    chain_id: Vec<u64>,
    n_rows: usize,
}

impl NativeTransfersColumns {
    fn new(capacity: usize) -> NativeTransfersColumns {
        NativeTransfersColumns {
            block_number: Vec::with_capacity(capacity),
            transaction_index: Vec::with_capacity(capacity),
            transfer_index: Vec::with_capacity(capacity),
            transaction_hash: Vec::with_capacity(capacity),
            from_address: Vec::with_capacity(capacity),
            to_address: Vec::with_capacity(capacity),
            value: Vec::with_capacity(capacity),
            chain_id: Vec::with_capacity(capacity),
            n_rows: 0,
        }
    }

    fn into_df(self, schema: &Table, chain_id: u64) -> Result<DataFrame, CollectError> {
        let mut cols = Vec::new();

        with_series!(cols, "block_number", self.block_number, schema);
        with_series!(cols, "transaction_index", self.transaction_index, schema);
        with_series!(cols, "transfer_index", self.transfer_index, schema);
        with_series_binary!(cols, "transaction_hash", self.transaction_hash, schema);
        with_series_binary!(cols, "from_address", self.from_address, schema);
        with_series_binary!(cols, "to_address", self.to_address, schema);
        with_series_binary!(cols, "value", self.value, schema);
        with_series!(cols, "chain_id", self.chain_id, schema);

        if schema.has_column("chain_id") {
            cols.push(Series::new("chain_id", vec![chain_id; self.n_rows]));
        };

        DataFrame::new(cols).map_err(CollectError::PolarsError).sort_by_schema(schema)
    }
}

async fn traces_to_native_transfers_df(
    mut rx: mpsc::Receiver<Result<Vec<Trace>, CollectError>>,
    schema: &Table,
    chain_id: u64,
) -> Result<DataFrame, CollectError> {
    let mut columns = NativeTransfersColumns::new(200);
    while let Some(message) = rx.recv().await {
        match message {
            Ok(traces) => process_traces(traces, schema, &mut columns)?,
            _ => return Err(CollectError::TooManyRequestsError),
        }
    }
    columns.into_df(schema, chain_id)
}

fn process_traces(
    traces: Vec<Trace>,
    schema: &Table,
    columns: &mut NativeTransfersColumns,
) -> Result<(), CollectError> {
    for (transfer_index, trace) in traces.iter().enumerate() {
        columns.n_rows += 1;

        if schema.has_column("block_number") {
            columns.block_number.push(trace.block_number);
        }
        if schema.has_column("transaction_index") {
            match trace.transaction_position {
                Some(index) => {
                    columns.transaction_index.push(Some(index as u32));
                }
                None => {
                    columns.transaction_index.push(None);
                }
            };
        };
        if schema.has_column("transfer_index") {
            columns.transfer_index.push(transfer_index as u32);
        };
        if schema.has_column("transaction_hash") {
            match trace.transaction_hash {
                Some(hash) => {
                    columns.transaction_hash.push(Some(hash.as_bytes().into()));
                }
                None => {
                    columns.transaction_hash.push(None);
                }
            };
        };
        match &trace.action {
            Action::Call(call) => {
                if schema.has_column("from_address") {
                    columns.from_address.push(call.from.0.into());
                }
                if schema.has_column("to_address") {
                    columns.to_address.push(call.to.0.into());
                }
                if schema.has_column("value") {
                    columns.value.push(call.value.to_vec_u8());
                }
            }
            Action::Create(create) => {
                if schema.has_column("from_address") {
                    columns.from_address.push(create.from.0.into());
                }
                if schema.has_column("to_address") {
                    match &trace.result {
                        Some(Res::Create(res)) => columns.to_address.push(res.address.0.into()),
                        _ => {
                            return Err(CollectError::CollectError(
                                "missing create result".to_string(),
                            ))
                        }
                    }
                }
                if schema.has_column("value") {
                    columns.value.push(create.value.to_vec_u8());
                }
            }
            Action::Suicide(suicide) => {
                if schema.has_column("from_address") {
                    columns.from_address.push(suicide.address.0.into());
                }
                if schema.has_column("to_address") {
                    columns.to_address.push(suicide.refund_address.0.into());
                }
                if schema.has_column("value") {
                    columns.value.push(suicide.balance.to_vec_u8());
                }
            }
            Action::Reward(reward) => {
                if schema.has_column("from_address") {
                    columns.from_address.push(vec![0; 20]);
                }
                if schema.has_column("to_address") {
                    columns.to_address.push(reward.author.0.into());
                }
                if schema.has_column("value") {
                    columns.value.push(reward.value.to_vec_u8());
                }
            }
        }
    }

    Ok(())
}
