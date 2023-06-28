use std::collections::HashMap;
use std::sync::Arc;

use ethers::prelude::*;
use futures::future::join_all;
use polars::prelude::*;
use tokio::sync::Semaphore;

use crate::chunks;
use crate::types::BlockChunk;
use crate::types::ColumnType;
use crate::types::Dataset;
use crate::types::FreezeOpts;
use crate::types::Logs;

#[async_trait::async_trait]
impl Dataset for Logs {
    fn name(&self) -> &'static str {
        "logs"
    }

    fn column_types(&self) -> HashMap<&'static str, ColumnType> {
        HashMap::from_iter(vec![
            ("block_number", ColumnType::Int32),
            ("transaction_index", ColumnType::Int32),
            ("log_index", ColumnType::Int32),
            ("transaction_hash", ColumnType::Binary),
            ("contract_address", ColumnType::Binary),
            ("topic0", ColumnType::Binary),
            ("topic1", ColumnType::Binary),
            ("topic2", ColumnType::Binary),
            ("topic3", ColumnType::Binary),
            ("data", ColumnType::Binary),
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

    async fn collect_dataset(&self, block_chunk: &BlockChunk, opts: &FreezeOpts) -> DataFrame {
        let logs = get_logs(
            block_chunk,
            &opts.contract,
            &[
                opts.topic0.clone(),
                opts.topic1.clone(),
                opts.topic2.clone(),
                opts.topic3.clone(),
            ],
            opts,
        )
        .await
        .unwrap();
        logs_to_df(logs).unwrap()
    }
}

pub async fn get_logs(
    block_chunk: &BlockChunk,
    address: &Option<ValueOrArray<H160>>,
    topics: &[Option<ValueOrArray<Option<H256>>>; 4],
    opts: &FreezeOpts,
) -> Result<Vec<Log>, Box<dyn std::error::Error>> {
    let request_chunks = chunks::block_chunk_to_filter_options(block_chunk, &opts.log_request_size);
    let results = fetch_logs(
        request_chunks,
        address,
        &topics,
        &opts.provider,
        &opts.max_concurrent_blocks,
    )
    .await
    .unwrap();

    let mut logs: Vec<Log> = Vec::new();
    for result in results {
        for log in result.unwrap() {
            logs.push(log);
        }
    }

    Ok(logs)
}

pub async fn fetch_logs(
    request_chunks: Vec<FilterBlockOption>,
    address: &Option<ValueOrArray<H160>>,
    topics: &[Option<ValueOrArray<Option<H256>>>; 4],
    provider: &Provider<Http>,
    max_concurrent_requests: &u64,
) -> Result<Vec<Option<Vec<Log>>>, Box<dyn std::error::Error>> {
    let semaphore = Arc::new(Semaphore::new(*max_concurrent_requests as usize));

    // prepare futures for concurrent execution
    let futures = request_chunks.into_iter().map(|request_chunk| {
        let provider = provider.clone();
        let semaphore = Arc::clone(&semaphore); // Cloning the Arc, not the Semaphore
        let filter = Filter {
            block_option: request_chunk,
            address: address.clone(),
            topics: topics.clone(),
        };
        tokio::spawn(async move {
            let permit = Arc::clone(&semaphore).acquire_owned().await;
            let result = provider.get_logs(&filter).await;
            drop(permit); // release the permit when the task is done
            result
        })
    });

    let results: Result<Vec<Option<Vec<Log>>>, _> = join_all(futures)
        .await
        .into_iter()
        .map(|r| match r {
            Ok(Ok(block)) => Ok(Some(block)),
            Ok(Err(e)) => {
                println!("Failed to get block: {}", e);
                Ok(None)
            }
            Err(e) => Err(format!("Task failed: {}", e)),
        })
        .collect();

    match results {
        Ok(blocks) => Ok(blocks),
        Err(e) => Err(Box::from(e)), // Convert the error into a boxed dyn Error
    }
}

pub fn logs_to_df(logs: Vec<Log>) -> Result<DataFrame, Box<dyn std::error::Error>> {
    // not recording: block_hash, transaction_log_index
    let mut address: Vec<Vec<u8>> = Vec::new();
    let mut topic0: Vec<Option<Vec<u8>>> = Vec::new();
    let mut topic1: Vec<Option<Vec<u8>>> = Vec::new();
    let mut topic2: Vec<Option<Vec<u8>>> = Vec::new();
    let mut topic3: Vec<Option<Vec<u8>>> = Vec::new();
    let mut data: Vec<Vec<u8>> = Vec::new();
    let mut block_number: Vec<u64> = Vec::new();
    let mut transaction_hash: Vec<Vec<u8>> = Vec::new();
    let mut transaction_index: Vec<u64> = Vec::new();
    let mut log_index: Vec<u64> = Vec::new();
    // let mut log_type: Vec<Option<String>> = Vec::new();

    for log in logs.iter() {
        if log.removed.unwrap() {
            continue;
        };

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
            _ => {
                panic!("Invalid number of topics");
            }
        }
        data.push(log.data.clone().to_vec());
        block_number.push(log.block_number.unwrap().as_u64());
        transaction_hash.push(
            log.transaction_hash
                .map(|hash| hash.as_bytes().to_vec())
                .unwrap(),
        );
        transaction_index.push(log.transaction_index.unwrap().as_u64());
        log_index.push(log.log_index.unwrap().as_u64());
        // log_type.push(log.log_type.clone());
    }

    let df = df!(
        "block_number" => block_number,
        "transaction_index" => transaction_index,
        "log_index" => log_index,
        "transaction_hash" => transaction_hash,
        "contract_address" => address,
        "topic0" => topic0,
        "topic1" => topic1,
        "topic2" => topic2,
        "topic3" => topic3,
        "data" => data,
        // "log_type" => log_type,
    );

    Ok(df?)
}
