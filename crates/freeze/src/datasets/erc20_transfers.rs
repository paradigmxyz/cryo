// 0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef
// data field is a single 32 byte word

// single erc20 / many erc20s
// single address / many addresses
// single block / many blocks

// --contract(s)
// --address(es)

use std::collections::HashMap;

use ethers::prelude::*;
use polars::prelude::*;
use tokio::sync::mpsc;

use crate::{
    dataframes::SortableDataFrame,
    types::{
        conversions::{ToVecHex, ToVecU8},
        BlockChunk, CollectError, ColumnType, Dataset, Datatype, RowFilter, Source, Table,
        TransactionChunk,
    },
    with_series, with_series_binary, with_series_u256, ColumnEncoding,
};

use super::logs;
use crate::{types::Erc20Transfers, U256Type};

#[async_trait::async_trait]
impl Dataset for Erc20Transfers {
    fn datatype(&self) -> Datatype {
        Datatype::Erc20Transfers
    }

    fn name(&self) -> &'static str {
        "erc20_transfers"
    }

    fn column_types(&self) -> HashMap<&'static str, ColumnType> {
        HashMap::from_iter(vec![
            ("block_number", ColumnType::UInt32),
            ("transaction_index", ColumnType::UInt32),
            ("log_index", ColumnType::UInt32),
            ("transaction_hash", ColumnType::Binary),
            ("erc20", ColumnType::Binary),
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
            "log_index",
            "transaction_hash",
            "erc20",
            "from_address",
            "to_address",
            "value",
            "chain_id",
        ]
    }

    fn default_sort(&self) -> Vec<String> {
        vec!["block_number".to_string(), "log_index".to_string()]
    }

    async fn collect_block_chunk(
        &self,
        chunk: &BlockChunk,
        source: &Source,
        schema: &Table,
        filter: Option<&RowFilter>,
    ) -> Result<DataFrame, CollectError> {
        let filter = get_row_filter(filter);
        let rx = logs::fetch_block_logs(chunk, source, Some(&filter)).await;
        logs_to_erc20_transfers(rx, schema, source.chain_id).await
    }

    async fn collect_transaction_chunk(
        &self,
        chunk: &TransactionChunk,
        source: &Source,
        schema: &Table,
        filter: Option<&RowFilter>,
    ) -> Result<DataFrame, CollectError> {
        let filter = get_row_filter(filter);
        let rx = logs::fetch_transaction_logs(chunk, source, Some(&filter)).await;
        logs_to_erc20_transfers(rx, schema, source.chain_id).await
    }
}

pub(crate) fn get_row_filter(filter: Option<&RowFilter>) -> RowFilter {
    let event_hash: H256 = H256(
        prefix_hex::decode("0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef")
            .expect("Decoding failed"),
    );
    let transfer_topics: [Option<ValueOrArray<Option<H256>>>; 4] =
        [Some(ValueOrArray::Value(Some(event_hash))), None, None, None];
    match filter {
        None => RowFilter { topics: transfer_topics, ..Default::default() },
        Some(filter) => RowFilter { topics: transfer_topics, ..filter.clone() },
    }
}

#[derive(Default)]
pub(crate) struct Erc20TransferLogColumns {
    n_rows: usize,
    block_number: Vec<u32>,
    transaction_index: Vec<u32>,
    log_index: Vec<u32>,
    transaction_hash: Vec<Vec<u8>>,
    erc20: Vec<Vec<u8>>,
    from_address: Vec<Vec<u8>>,
    to_address: Vec<Vec<u8>>,
    value: Vec<U256>,
}

impl Erc20TransferLogColumns {
    pub(crate) fn process_erc20_transfer_logs(
        &mut self,
        logs: Vec<Log>,
        schema: &Table,
    ) -> Result<(), CollectError> {
        for log in &logs {
            if let Some(true) = log.removed {
                continue
            }
            if log.data.is_empty() {
                continue
            }
            if let (Some(bn), Some(tx), Some(ti), Some(li)) =
                (log.block_number, log.transaction_hash, log.transaction_index, log.log_index)
            {
                self.n_rows += 1;
                if schema.has_column("block_number") {
                    self.block_number.push(bn.as_u32());
                };
                if schema.has_column("transaction_index") {
                    self.transaction_index.push(ti.as_u32());
                };
                if schema.has_column("log_index") {
                    self.log_index.push(li.as_u32());
                };
                if schema.has_column("transaction_hash") {
                    self.transaction_hash.push(tx.as_bytes().to_vec());
                };
                if schema.has_column("erc20") {
                    self.erc20.push(log.address.as_bytes().to_vec());
                };
                if schema.has_column("from_address") {
                    self.from_address.push(log.topics[1].as_bytes().to_vec());
                };
                if schema.has_column("to_address") {
                    self.to_address.push(log.topics[2].as_bytes().to_vec());
                };
                if schema.has_column("value") {
                    self.value.push(log.data.to_vec().as_slice().into());
                };
            }
        }

        Ok(())
    }

    pub(crate) fn create_df(
        self,
        schema: &Table,
        chain_id: u64,
    ) -> Result<DataFrame, CollectError> {
        let mut cols = Vec::with_capacity(schema.columns().len());
        with_series!(cols, "block_number", self.block_number, schema);
        with_series!(cols, "transaction_index", self.transaction_index, schema);
        with_series!(cols, "log_index", self.log_index, schema);
        with_series_binary!(cols, "transaction_hash", self.transaction_hash, schema);
        with_series_binary!(cols, "erc20", self.erc20, schema);
        with_series_binary!(cols, "from_address", self.from_address, schema);
        with_series_binary!(cols, "to_address", self.to_address, schema);
        with_series_u256!(cols, "value", self.value, schema);
        with_series!(cols, "chain_id", vec![chain_id; self.n_rows], schema);

        DataFrame::new(cols).map_err(CollectError::PolarsError).sort_by_schema(schema)
    }
}

async fn logs_to_erc20_transfers(
    mut logs: mpsc::Receiver<Result<Vec<Log>, CollectError>>,
    schema: &Table,
    chain_id: u64,
) -> Result<DataFrame, CollectError> {
    let mut columns = Erc20TransferLogColumns::default();
    while let Some(message) = logs.recv().await {
        if let Ok(logs) = message {
            columns.process_erc20_transfer_logs(logs, schema)?
        } else {
            return Err(CollectError::TooManyRequestsError)
        }
    }
    columns.create_df(schema, chain_id)
}
