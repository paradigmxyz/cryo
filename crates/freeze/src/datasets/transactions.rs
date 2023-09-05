use std::collections::HashMap;

use ethers::prelude::*;
use polars::prelude::*;
use tokio::{sync::mpsc, task};

use super::{blocks, blocks::ProcessTransactions, blocks_and_transactions};
use crate::{
    dataframes::SortableDataFrame,
    types::{
        conversions::{ToVecHex, ToVecU8},
        BlockChunk, CollectError, ColumnEncoding, ColumnType, Dataset, Datatype, RowFilter, Source,
        Table, TransactionChunk, Transactions, U256Type,
    },
    with_series, with_series_binary, with_series_u256,
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
            ("value", ColumnType::UInt256),
            ("input", ColumnType::Binary),
            ("gas_limit", ColumnType::UInt32),
            ("gas_used", ColumnType::UInt32),
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
        let include_gas_used = schema.has_column("gas_used");
        let rx =
            blocks_and_transactions::fetch_blocks_and_transactions(chunk, source, include_gas_used)
                .await;
        let output = blocks::blocks_to_dfs(rx, &None, &Some(schema), source.chain_id).await;
        match output {
            Ok((_, Some(txs_df))) => Ok(txs_df),
            Ok((_, _)) => Err(CollectError::BadSchemaError),
            Err(e) => Err(e),
        }
    }

    async fn collect_transaction_chunk(
        &self,
        chunk: &TransactionChunk,
        source: &Source,
        schema: &Table,
        _filter: Option<&RowFilter>,
    ) -> Result<DataFrame, CollectError> {
        let include_gas_used = schema.has_column("gas_used");
        let rx = fetch_transactions(chunk, source, include_gas_used).await;
        transactions_to_df(rx, schema, source.chain_id).await
    }
}

async fn fetch_transactions(
    transaction_chunk: &TransactionChunk,
    source: &Source,
    include_gas_used: bool,
) -> mpsc::Receiver<Result<(Transaction, Option<u32>), CollectError>> {
    let (tx, rx) = mpsc::channel(1);

    match transaction_chunk {
        TransactionChunk::Values(tx_hashes) => {
            for tx_hash in tx_hashes.iter() {
                let tx = tx.clone();
                let fetcher = source.fetcher.clone();
                let tx_hash = tx_hash.clone();
                task::spawn(async move {
                    let tx_hash = H256::from_slice(&tx_hash);
                    let transaction = fetcher.get_transaction(tx_hash).await;

                    // get gas_used using receipt
                    let gas_used = if include_gas_used {
                        match fetcher.get_transaction_receipt(tx_hash).await? {
                            Some(receipt) => match receipt.gas_used {
                                Some(gas_used) => Some(gas_used.as_u32()),
                                None => {
                                    return Err(CollectError::CollectError(
                                        "gas_used not found in receipt".to_string(),
                                    ))
                                }
                            },
                            _ => {
                                return Err(CollectError::CollectError(
                                    "could not get tx receipt".to_string(),
                                ))
                            }
                        }
                    } else {
                        None
                    };

                    // package result
                    let result = match transaction? {
                        Some(transaction) => Ok((transaction, gas_used)),
                        None => {
                            Err(CollectError::CollectError("transaction not in node".to_string()))
                        }
                    };

                    // send to channel
                    match tx.send(result).await {
                        Ok(_) => Ok(()),
                        Err(tokio::sync::mpsc::error::SendError(_e)) => {
                            eprintln!("send error, try using a rate limit with --requests-per-second or limiting max concurrency with --max-concurrent-requests");
                            std::process::exit(1)
                        }
                    }
                });
            }
            rx
        }
        _ => rx,
    }
}

#[derive(Default)]
pub(crate) struct TransactionColumns {
    n_rows: usize,
    block_number: Vec<Option<u64>>,
    transaction_index: Vec<Option<u64>>,
    transaction_hash: Vec<Vec<u8>>,
    nonce: Vec<u64>,
    from_address: Vec<Vec<u8>>,
    to_address: Vec<Option<Vec<u8>>>,
    value: Vec<U256>,
    input: Vec<Vec<u8>>,
    gas_limit: Vec<u32>,
    gas_used: Vec<u32>,
    gas_price: Vec<Option<u64>>,
    transaction_type: Vec<Option<u32>>,
    max_priority_fee_per_gas: Vec<Option<u64>>,
    max_fee_per_gas: Vec<Option<u64>>,
}

impl TransactionColumns {
    pub(crate) fn create_df(
        self,
        schema: &Table,
        chain_id: u64,
    ) -> Result<DataFrame, CollectError> {
        let mut cols = Vec::with_capacity(schema.columns().len());
        with_series!(cols, "block_number", self.block_number, schema);
        with_series!(cols, "transaction_index", self.transaction_index, schema);
        with_series_binary!(cols, "transaction_hash", self.transaction_hash, schema);
        with_series!(cols, "nonce", self.nonce, schema);
        with_series_binary!(cols, "from_address", self.from_address, schema);
        with_series_binary!(cols, "to_address", self.to_address, schema);
        with_series_u256!(cols, "value", self.value, schema);
        with_series_binary!(cols, "input", self.input, schema);
        with_series!(cols, "gas_limit", self.gas_limit, schema);
        with_series!(cols, "gas_used", self.gas_used, schema);
        with_series!(cols, "gas_price", self.gas_price, schema);
        with_series!(cols, "transaction_type", self.transaction_type, schema);
        with_series!(cols, "max_priority_fee_per_gas", self.max_priority_fee_per_gas, schema);
        with_series!(cols, "max_fee_per_gas", self.max_fee_per_gas, schema);
        with_series!(cols, "chain_id", vec![chain_id; self.n_rows], schema);

        DataFrame::new(cols).map_err(CollectError::PolarsError).sort_by_schema(schema)
    }

    pub(crate) fn process_transaction(
        &mut self,
        tx: &Transaction,
        schema: &Table,
        gas_used: Option<u32>,
    ) {
        self.n_rows += 1;
        if schema.has_column("block_number") {
            match tx.block_number {
                Some(block_number) => self.block_number.push(Some(block_number.as_u64())),
                None => self.block_number.push(None),
            }
        }
        if schema.has_column("transaction_index") {
            match tx.transaction_index {
                Some(transaction_index) => {
                    self.transaction_index.push(Some(transaction_index.as_u64()))
                }
                None => self.transaction_index.push(None),
            }
        }
        if schema.has_column("transaction_hash") {
            self.transaction_hash.push(tx.hash.as_bytes().to_vec());
        }
        if schema.has_column("from_address") {
            self.from_address.push(tx.from.as_bytes().to_vec());
        }
        if schema.has_column("to_address") {
            match tx.to {
                Some(to_address) => self.to_address.push(Some(to_address.as_bytes().to_vec())),
                None => self.to_address.push(None),
            }
        }
        if schema.has_column("nonce") {
            self.nonce.push(tx.nonce.as_u64());
        }
        if schema.has_column("value") {
            self.value.push(tx.value);
        }
        if schema.has_column("input") {
            self.input.push(tx.input.to_vec());
        }
        if schema.has_column("gas_limit") {
            self.gas_limit.push(tx.gas.as_u32());
        }
        if schema.has_column("gas_used") {
            self.gas_used.push(gas_used.unwrap())
        }
        if schema.has_column("gas_price") {
            self.gas_price.push(tx.gas_price.map(|gas_price| gas_price.as_u64()));
        }
        if schema.has_column("transaction_type") {
            self.transaction_type.push(tx.transaction_type.map(|value| value.as_u32()));
        }
        if schema.has_column("max_priority_fee_per_gas") {
            self.max_priority_fee_per_gas
                .push(tx.max_priority_fee_per_gas.map(|value| value.as_u64()));
        }
        if schema.has_column("max_fee_per_gas") {
            self.max_fee_per_gas.push(tx.max_fee_per_gas.map(|value| value.as_u64()));
        }
    }
}

async fn transactions_to_df(
    mut transactions: mpsc::Receiver<Result<(Transaction, Option<u32>), CollectError>>,
    schema: &Table,
    chain_id: u64,
) -> Result<DataFrame, CollectError> {
    let mut columns = TransactionColumns::default();
    while let Some(message) = transactions.recv().await {
        match message {
            Ok((transaction, gas_used)) => transaction.process(schema, &mut columns, gas_used),
            Err(e) => return Err(e),
        }
    }
    columns.create_df(schema, chain_id)
}
