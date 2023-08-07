use std::collections::HashMap;

use ethers::prelude::*;
use polars::prelude::*;
use tokio::{sync::mpsc, task};

use super::{blocks, blocks::ProcessTransactions, blocks_and_transactions};
use crate::types::{
    BlockChunk, CollectError, ColumnType, Dataset, Datatype, RowFilter, Source, Table,
    TransactionChunk, Transactions,
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
                let provider = Arc::clone(&source.provider);
                let semaphore = source.semaphore.clone();
                let rate_limiter = source.rate_limiter.as_ref().map(Arc::clone);
                let tx_hash = tx_hash.clone();
                task::spawn(async move {
                    let _permit = match semaphore {
                        Some(semaphore) => Some(Arc::clone(&semaphore).acquire_owned().await),
                        _ => None,
                    };
                    if let Some(limiter) = rate_limiter {
                        Arc::clone(&limiter).until_ready().await;
                    }
                    let tx_hash = H256::from_slice(&tx_hash);
                    let transaction = provider.get_transaction(tx_hash).await;

                    // get gas_used using receipt
                    let gas_used = if include_gas_used {
                        match provider.get_transaction_receipt(tx_hash).await {
                            Ok(Some(receipt)) => match receipt.gas_used {
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
                    let result = match transaction {
                        Ok(Some(transaction)) => Ok((transaction, gas_used)),
                        Ok(None) => {
                            Err(CollectError::CollectError("transaction not in node".to_string()))
                        }
                        Err(e) => Err(CollectError::ProviderError(e)),
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

async fn transactions_to_df(
    mut transactions: mpsc::Receiver<Result<(Transaction, Option<u32>), CollectError>>,
    schema: &Table,
    chain_id: u64,
) -> Result<DataFrame, CollectError> {
    let mut columns = blocks::TransactionColumns::new(100);
    let mut n_txs = 0;
    while let Some(message) = transactions.recv().await {
        match message {
            Ok((transaction, gas_used)) => {
                n_txs += 1;
                transaction.process(schema, &mut columns, gas_used)
            }
            Err(e) => return Err(e),
        }
    }
    columns.create_df(schema, chain_id, n_txs)
}
