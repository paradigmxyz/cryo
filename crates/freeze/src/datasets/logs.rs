use std::{collections::HashMap, sync::Arc};

use ethers::prelude::*;
use polars::prelude::*;
use tokio::{sync::mpsc, task};

use crate::{
    dataframes::SortableDataFrame,
    types::{
        conversions::ToVecHex, BlockChunk, CollectError, ColumnType, Dataset, Datatype, Logs,
        RowFilter, Source, Table, TransactionChunk,
    },
    with_series, with_series_binary,
};

#[async_trait::async_trait]
impl Dataset for Logs {
    fn datatype(&self) -> Datatype {
        Datatype::Logs
    }

    fn name(&self) -> &'static str {
        "logs"
    }

    fn column_types(&self) -> HashMap<&'static str, ColumnType> {
        HashMap::from_iter(vec![
            ("block_number", ColumnType::UInt32),
            ("transaction_index", ColumnType::UInt32),
            ("log_index", ColumnType::UInt32),
            ("transaction_hash", ColumnType::Binary),
            ("contract_address", ColumnType::Binary),
            ("topic0", ColumnType::Binary),
            ("topic1", ColumnType::Binary),
            ("topic2", ColumnType::Binary),
            ("topic3", ColumnType::Binary),
            ("data", ColumnType::Binary),
            ("chain_id", ColumnType::UInt64),
        ])
    }

    fn default_columns(&self) -> Vec<&'static str> {
        vec![
            "block_number",
            "transaction_index",
            "log_index",
            "transaction_hash",
            "contract_address",
            "topic0",
            "topic1",
            "topic2",
            "topic3",
            "data",
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
        let rx = fetch_block_logs(chunk, source, filter).await;
        logs_to_df(rx, schema, source.chain_id).await
    }

    async fn collect_transaction_chunk(
        &self,
        chunk: &TransactionChunk,
        source: &Source,
        schema: &Table,
        filter: Option<&RowFilter>,
    ) -> Result<DataFrame, CollectError> {
        // if let Some(_filter) = filter {
        //     return Err(CollectError::CollectError(
        //         "filters not supported when using --txs".to_string(),
        //     ));
        // };
        let rx = fetch_transaction_logs(chunk, source, filter).await;
        logs_to_df(rx, schema, source.chain_id).await
    }
}

async fn fetch_block_logs(
    block_chunk: &BlockChunk,
    source: &Source,
    filter: Option<&RowFilter>,
) -> mpsc::Receiver<Result<Vec<Log>, CollectError>> {
    // todo: need to modify these functions so they turn a result
    let request_chunks = block_chunk.to_log_filter_options(&source.inner_request_size);
    let (tx, rx) = mpsc::channel(request_chunks.len());
    for request_chunk in request_chunks.iter() {
        let tx = tx.clone();
        let provider = source.provider.clone();
        let semaphore = source.semaphore.clone();
        let rate_limiter = source.rate_limiter.as_ref().map(Arc::clone);
        let log_filter = match filter {
            Some(filter) => Filter {
                block_option: *request_chunk,
                address: filter.address.clone(),
                topics: filter.topics.clone(),
            },
            None => Filter {
                block_option: *request_chunk,
                address: None,
                topics: [None, None, None, None],
            },
        };
        task::spawn(async move {
            let _permit = match semaphore {
                Some(semaphore) => Some(Arc::clone(&semaphore).acquire_owned().await),
                _ => None,
            };
            if let Some(limiter) = rate_limiter {
                Arc::clone(&limiter).until_ready().await;
            }
            let result = provider.get_logs(&log_filter).await.map_err(CollectError::ProviderError);
            match tx.send(result).await {
                Ok(_) => {}
                Err(tokio::sync::mpsc::error::SendError(_e)) => {
                    eprintln!("send error, try using a rate limit with --requests-per-second or limiting max concurrency with --max-concurrent-requests");
                    std::process::exit(1)
                }
            }
        });
    }
    rx
}

async fn fetch_transaction_logs(
    transaction_chunk: &TransactionChunk,
    source: &Source,
    _filter: Option<&RowFilter>,
) -> mpsc::Receiver<Result<Vec<Log>, CollectError>> {
    match transaction_chunk {
        TransactionChunk::Values(tx_hashes) => {
            let (tx, rx) = mpsc::channel(tx_hashes.len() * 200);
            for tx_hash in tx_hashes.iter() {
                let tx_hash = tx_hash.clone();
                let tx = tx.clone();
                let provider = source.provider.clone();
                let semaphore = source.semaphore.clone();
                let rate_limiter = source.rate_limiter.as_ref().map(Arc::clone);
                task::spawn(async move {
                    let _permit = match semaphore {
                        Some(semaphore) => Some(Arc::clone(&semaphore).acquire_owned().await),
                        _ => None,
                    };
                    if let Some(limiter) = rate_limiter {
                        Arc::clone(&limiter).until_ready().await;
                    }
                    let receipt = provider
                        .get_transaction_receipt(H256::from_slice(&tx_hash))
                        .await
                        .map_err(CollectError::ProviderError);
                    let logs = match receipt {
                        Ok(Some(receipt)) => Ok(receipt.logs),
                        _ => Err(CollectError::CollectError("".to_string())),
                    };
                    match tx.send(logs).await {
                        Ok(_) => {}
                        Err(tokio::sync::mpsc::error::SendError(_e)) => {
                            eprintln!("send error, try using a rate limit with --requests-per-second or limiting max concurrency with --max-concurrent-requests");
                            std::process::exit(1)
                        }
                    }
                });
            }
            rx
        }
        _ => {
            let (tx, rx) = mpsc::channel(1);
            let result = Err(CollectError::CollectError(
                "transaction value ranges not supported".to_string(),
            ));
            match tx.send(result).await {
                Ok(_) => {}
                Err(tokio::sync::mpsc::error::SendError(_e)) => {
                    eprintln!("send error, try using a rate limit with --requests-per-second or limiting max concurrency with --max-concurrent-requests");
                    std::process::exit(1)
                }
            }
            rx
        }
    }
}

#[derive(Default)]
pub(crate) struct LogColumns {
    n_rows: usize,
    block_number: Vec<u32>,
    transaction_index: Vec<u32>,
    log_index: Vec<u32>,
    transaction_hash: Vec<Vec<u8>>,
    address: Vec<Vec<u8>>,
    topic0: Vec<Option<Vec<u8>>>,
    topic1: Vec<Option<Vec<u8>>>,
    topic2: Vec<Option<Vec<u8>>>,
    topic3: Vec<Option<Vec<u8>>>,
    data: Vec<Vec<u8>>,
}

impl LogColumns {
    pub(crate) fn process_logs(
        &mut self,
        logs: Vec<Log>,
        schema: &Table,
    ) -> Result<(), CollectError> {
        for log in logs {
            if let Some(true) = log.removed {
                continue
            }
            if let (Some(bn), Some(tx), Some(ti), Some(li)) =
                (log.block_number, log.transaction_hash, log.transaction_index, log.log_index)
            {
                self.n_rows += 1;
                self.address.push(log.address.as_bytes().to_vec());
                match log.topics.len() {
                    0 => {
                        self.topic0.push(None);
                        self.topic1.push(None);
                        self.topic2.push(None);
                        self.topic3.push(None);
                    }
                    1 => {
                        self.topic0.push(Some(log.topics[0].as_bytes().to_vec()));
                        self.topic1.push(None);
                        self.topic2.push(None);
                        self.topic3.push(None);
                    }
                    2 => {
                        self.topic0.push(Some(log.topics[0].as_bytes().to_vec()));
                        self.topic1.push(Some(log.topics[1].as_bytes().to_vec()));
                        self.topic2.push(None);
                        self.topic3.push(None);
                    }
                    3 => {
                        self.topic0.push(Some(log.topics[0].as_bytes().to_vec()));
                        self.topic1.push(Some(log.topics[1].as_bytes().to_vec()));
                        self.topic2.push(Some(log.topics[2].as_bytes().to_vec()));
                        self.topic3.push(None);
                    }
                    4 => {
                        self.topic0.push(Some(log.topics[0].as_bytes().to_vec()));
                        self.topic1.push(Some(log.topics[1].as_bytes().to_vec()));
                        self.topic2.push(Some(log.topics[2].as_bytes().to_vec()));
                        self.topic3.push(Some(log.topics[3].as_bytes().to_vec()));
                    }
                    _ => return Err(CollectError::InvalidNumberOfTopics),
                }
                if schema.has_column("data") {
                    self.data.push(log.data.to_vec());
                }
                self.block_number.push(bn.as_u32());
                self.transaction_hash.push(tx.as_bytes().to_vec());
                self.transaction_index.push(ti.as_u32());
                self.log_index.push(li.as_u32());
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
        with_series_binary!(cols, "contract_address", self.address, schema);
        with_series_binary!(cols, "topic0", self.topic0, schema);
        with_series_binary!(cols, "topic1", self.topic1, schema);
        with_series_binary!(cols, "topic2", self.topic2, schema);
        with_series_binary!(cols, "topic3", self.topic3, schema);
        with_series_binary!(cols, "data", self.data, schema);
        with_series!(cols, "chain_id", vec![chain_id; self.n_rows], schema);

        DataFrame::new(cols).map_err(CollectError::PolarsError).sort_by_schema(schema)
    }
}

async fn logs_to_df(
    mut logs: mpsc::Receiver<Result<Vec<Log>, CollectError>>,
    schema: &Table,
    chain_id: u64,
) -> Result<DataFrame, CollectError> {
    let mut columns = LogColumns::default();

    while let Some(message) = logs.recv().await {
        if let Ok(logs) = message {
            columns.process_logs(logs, schema)?
        } else {
            return Err(CollectError::TooManyRequestsError)
        }
    }
    columns.create_df(schema, chain_id)
}
