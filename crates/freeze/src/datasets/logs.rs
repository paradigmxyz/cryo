use std::{collections::HashMap, sync::Arc};

use ethers::prelude::*;
use polars::prelude::*;
use tokio::{sync::mpsc, task};

use crate::{
    dataframes::SortableDataFrame,
    types::{
        conversions::ToVecHex, BlockChunk, CollectError, ColumnType, Dataset, Datatype, Logs,
        RowFilter, Source, Table,
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
        let rx = fetch_logs(chunk, source, filter).await;
        logs_to_df(rx, schema, source.chain_id).await
    }
}

async fn fetch_logs(
    block_chunk: &BlockChunk,
    source: &Source,
    filter: Option<&RowFilter>,
) -> mpsc::Receiver<Result<Vec<Log>, CollectError>> {
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

async fn logs_to_df(
    mut logs: mpsc::Receiver<Result<Vec<Log>, CollectError>>,
    schema: &Table,
    chain_id: u64,
) -> Result<DataFrame, CollectError> {
    let mut block_number: Vec<u32> = Vec::new();
    let mut transaction_index: Vec<u32> = Vec::new();
    let mut log_index: Vec<u32> = Vec::new();
    let mut transaction_hash: Vec<Vec<u8>> = Vec::new();
    let mut address: Vec<Vec<u8>> = Vec::new();
    let mut topic0: Vec<Option<Vec<u8>>> = Vec::new();
    let mut topic1: Vec<Option<Vec<u8>>> = Vec::new();
    let mut topic2: Vec<Option<Vec<u8>>> = Vec::new();
    let mut topic3: Vec<Option<Vec<u8>>> = Vec::new();
    let mut data: Vec<Vec<u8>> = Vec::new();

    let mut n_rows = 0;
    // while let Some(Ok(logs)) = logs.recv().await {
    while let Some(message) = logs.recv().await {
        match message {
            Ok(logs) => {
                for log in logs.iter() {
                    if let Some(true) = log.removed {
                        continue
                    }
                    if let (Some(bn), Some(tx), Some(ti), Some(li)) = (
                        log.block_number,
                        log.transaction_hash,
                        log.transaction_index,
                        log.log_index,
                    ) {
                        n_rows += 1;
                        address.push(log.address.as_bytes().to_vec());
                        match log.topics.len() {
                            0 => {
                                topic0.push(None);
                                topic1.push(None);
                                topic2.push(None);
                                topic3.push(None);
                            }
                            1 => {
                                topic0.push(Some(log.topics[0].as_bytes().to_vec()));
                                topic1.push(None);
                                topic2.push(None);
                                topic3.push(None);
                            }
                            2 => {
                                topic0.push(Some(log.topics[0].as_bytes().to_vec()));
                                topic1.push(Some(log.topics[1].as_bytes().to_vec()));
                                topic2.push(None);
                                topic3.push(None);
                            }
                            3 => {
                                topic0.push(Some(log.topics[0].as_bytes().to_vec()));
                                topic1.push(Some(log.topics[1].as_bytes().to_vec()));
                                topic2.push(Some(log.topics[2].as_bytes().to_vec()));
                                topic3.push(None);
                            }
                            4 => {
                                topic0.push(Some(log.topics[0].as_bytes().to_vec()));
                                topic1.push(Some(log.topics[1].as_bytes().to_vec()));
                                topic2.push(Some(log.topics[2].as_bytes().to_vec()));
                                topic3.push(Some(log.topics[3].as_bytes().to_vec()));
                            }
                            _ => return Err(CollectError::InvalidNumberOfTopics),
                        }
                        data.push(log.data.clone().to_vec());
                        block_number.push(bn.as_u32());
                        transaction_hash.push(tx.as_bytes().to_vec());
                        transaction_index.push(ti.as_u32());
                        log_index.push(li.as_u32());
                    }
                }
            }
            _ => return Err(CollectError::TooManyRequestsError),
        }
    }

    let mut cols = Vec::new();
    with_series!(cols, "block_number", block_number, schema);
    with_series!(cols, "transaction_index", transaction_index, schema);
    with_series!(cols, "log_index", log_index, schema);
    with_series_binary!(cols, "transaction_hash", transaction_hash, schema);
    with_series_binary!(cols, "contract_address", address, schema);
    with_series_binary!(cols, "topic0", topic0, schema);
    with_series_binary!(cols, "topic1", topic1, schema);
    with_series_binary!(cols, "topic2", topic2, schema);
    with_series_binary!(cols, "topic3", topic3, schema);
    with_series_binary!(cols, "data", data, schema);

    if schema.has_column("chain_id") {
        cols.push(Series::new("chain_id", vec![chain_id; n_rows]));
    }

    DataFrame::new(cols).map_err(CollectError::PolarsError).sort_by_schema(schema)
}
