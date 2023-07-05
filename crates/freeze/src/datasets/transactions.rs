use std::collections::HashMap;

use ethers::prelude::*;
use polars::prelude::*;
use tokio::sync::mpsc;
use tokio::task;

use crate::chunks::ChunkAgg;
use crate::types::BlockChunk;
use crate::types::CollectError;
use crate::types::ColumnType;
use crate::types::Dataset;
use crate::types::Datatype;
use crate::types::FetchOpts;
use crate::types::FreezeOpts;
use crate::types::Transactions;

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
            ("gas_limit", ColumnType::Int64),
            ("gas_price", ColumnType::Int64),
            ("transaction_type", ColumnType::Int32),
            ("max_priority_fee_per_gas", ColumnType::Int64),
            ("max_fee_per_gas", ColumnType::Int64),
            ("chain_id", ColumnType::Int64),
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

    async fn collect_chunk(
        &self,
        block_chunk: &BlockChunk,
        opts: &FreezeOpts,
    ) -> Result<DataFrame, CollectError> {
        let rx = fetch_blocks_and_transactions(block_chunk, &opts.chunk_fetch_opts()).await;
        txs_to_df(rx).await
    }
}

async fn fetch_blocks_and_transactions(
    block_chunk: &BlockChunk,
    opts: &FetchOpts,
) -> mpsc::Receiver<Result<Option<Block<Transaction>>, CollectError>> {
    let (tx, rx) = mpsc::channel(block_chunk.numbers().len());

    for number in block_chunk.numbers() {
        let tx = tx.clone();
        let provider = opts.provider.clone();
        let semaphore = opts.semaphore.clone();
        let rate_limiter = opts.rate_limiter.as_ref().map(Arc::clone);
        task::spawn(async move {
            let _permit = Arc::clone(&semaphore).acquire_owned().await;
            if let Some(limiter) = rate_limiter {
                Arc::clone(&limiter).until_ready().await;
            }
            let block = provider
                .get_block_with_txs(number)
                .await
                .map_err(CollectError::ProviderError);
            match tx.send(block).await {
                Ok(_) => {}
                Err(tokio::sync::mpsc::error::SendError(_e)) => println!("send error"),
            }
        });
    }
    rx
}

/// convert a `Vec<Transaction>` into polars dataframe
async fn txs_to_df(
    mut rx: mpsc::Receiver<Result<Option<Block<Transaction>>, CollectError>>,
) -> Result<DataFrame, CollectError> {
    // not recording: v, r, s, access_list
    let mut hashes: Vec<Vec<u8>> = Vec::new();
    let mut transaction_indices: Vec<Option<u64>> = Vec::new();
    let mut from_addresses: Vec<Vec<u8>> = Vec::new();
    let mut to_addresses: Vec<Option<Vec<u8>>> = Vec::new();
    let mut nonces: Vec<u64> = Vec::new();
    let mut block_numbers: Vec<Option<u64>> = Vec::new();
    let mut values: Vec<String> = Vec::new();
    let mut inputs: Vec<Vec<u8>> = Vec::new();
    let mut gas: Vec<u64> = Vec::new();
    let mut gas_price: Vec<Option<u64>> = Vec::new();
    let mut transaction_type: Vec<Option<u64>> = Vec::new();
    let mut max_priority_fee_per_gas: Vec<Option<u64>> = Vec::new();
    let mut max_fee_per_gas: Vec<Option<u64>> = Vec::new();
    let mut chain_ids: Vec<Option<u64>> = Vec::new();

    while let Some(Ok(Some(block))) = rx.recv().await {
        for tx in block.transactions.iter() {
            match tx.block_number {
                Some(block_number) => block_numbers.push(Some(block_number.as_u64())),
                None => block_numbers.push(None),
            }
            match tx.transaction_index {
                Some(transaction_index) => {
                    transaction_indices.push(Some(transaction_index.as_u64()))
                }
                None => transaction_indices.push(None),
            }
            hashes.push(tx.hash.as_bytes().to_vec());
            from_addresses.push(tx.from.as_bytes().to_vec());
            match tx.to {
                Some(to_address) => to_addresses.push(Some(to_address.as_bytes().to_vec())),
                None => to_addresses.push(None),
            }
            nonces.push(tx.nonce.as_u64());
            values.push(tx.value.to_string());
            inputs.push(tx.input.to_vec());
            gas.push(tx.gas.as_u64());
            gas_price.push(tx.gas_price.map(|gas_price| gas_price.as_u64()));
            transaction_type.push(tx.transaction_type.map(|value| value.as_u64()));
            max_priority_fee_per_gas.push(tx.max_priority_fee_per_gas.map(|value| value.as_u64()));
            max_fee_per_gas.push(tx.max_fee_per_gas.map(|value| value.as_u64()));
            chain_ids.push(tx.chain_id.map(|value| value.as_u64()));
        }
    }

    df!(
        "block_number" => block_numbers,
        "transaction_index" => transaction_indices,
        "transaction_hash" => hashes,
        "nonce" => nonces,
        "from_address" => from_addresses,
        "to_address" => to_addresses,
        "value" => values,
        "input" => inputs,
        "gas_limit" => gas,
        "gas_price" => gas_price,
        "transaction_type" => transaction_type,
        "max_priority_fee_per_gas" => max_priority_fee_per_gas,
        "max_fee_per_gas" => max_fee_per_gas,
        "chain_id" => chain_ids,
    )
    .map_err(CollectError::PolarsError)
}
