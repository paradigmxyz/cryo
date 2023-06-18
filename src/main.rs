// Questions
// - do connections get re-used in concurrent calls
// - is there a way to eliminate copying? (e.g. to_vec())
// - are most ethers functions async?
// Next goals
// - call from python, return result to python

use ethers::prelude::*;
use futures::future::join_all;
use polars::prelude::*;
use tokio::sync::Semaphore;

// const RPC_URL: &str = "https://eth.llamarpc.com";
const RPC_URL: &str = "http://34.105.67.70:8545";
const N_BLOCKS: u64 = 100;
const MAX_CONCURRENT_REQUESTS: usize = 100;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let provider = Provider::<Http>::try_from(RPC_URL)?;

    // get current block number
    let block_number: U64 = provider.get_block_number().await?;
    let start = block_number.as_u64().saturating_sub(N_BLOCKS - 1); // saturating_sub prevents underflow
    let block_numbers_to_fetch: Vec<_> = (start..=block_number.as_u64()).collect();

    // get txs
    let txs = get_blocks_txs(block_numbers_to_fetch, provider).await;

    // create dataframe
    let df: &mut DataFrame = &mut txs_to_df(txs.unwrap()).unwrap();

    // print dataframe
    println!("{:?}", df);
    println!("{:?}", df.schema());

    // write to file
    let filename = "parquet_writer_test.parquet";
    df_to_parquet(df, &filename);

    Ok(())
}

async fn get_blocks_txs(
    block_numbers: Vec<u64>,
    provider: Provider<Http>,
) -> Result<Vec<Transaction>, Box<dyn std::error::Error>> {
    /// fetch transactions of block_numbers from RPC node
    let semaphore = Arc::new(Semaphore::new(MAX_CONCURRENT_REQUESTS));

    // prepare futures for concurrent execution
    let futures = block_numbers.into_iter().map(|block_number| {
        let provider = provider.clone();
        let semaphore = Arc::clone(&semaphore); // Cloning the Arc, not the Semaphore
        tokio::spawn(async move {
            let permit = Arc::clone(&semaphore).acquire_owned().await;
            let result = provider.get_block_with_txs(block_number).await;
            drop(permit); // release the permit when the task is done
            result
        })
    });

    // execute all futures concurrently and collect the results
    let results: Vec<_> = join_all(futures)
        .await
        .into_iter()
        .map(|r| r.unwrap()) // unwrap tokio::task::JoinHandle results
        .collect();

    // Check the results for errors and unwrap successful results.
    // Map transactions from each block into a single, flat vector of transactions.
    let mut all_txs: Vec<Transaction> = Vec::new();
    for result in results {
        let block_with_txs = result?;
        let txs = block_with_txs.unwrap().transactions;
        all_txs.extend(txs);
    }

    Ok(all_txs)
}

fn txs_to_df(txs: Vec<Transaction>) -> Result<DataFrame, Box<dyn std::error::Error>> {
    /// convert a Vec<Transaction> into polars dataframe
    // not recording: v, r, s, access_list
    let mut hashes: Vec<&[u8]> = Vec::new();
    let mut transaction_indices: Vec<Option<u64>> = Vec::new();
    let mut from_addresses: Vec<&[u8]> = Vec::new();
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

    for tx in txs.iter() {
        match tx.block_number {
            Some(block_number) => block_numbers.push(Some(block_number.as_u64())),
            None => block_numbers.push(None),
        }
        match tx.transaction_index {
            Some(transaction_index) => transaction_indices.push(Some(transaction_index.as_u64())),
            None => transaction_indices.push(None),
        }
        hashes.push(tx.hash.as_bytes());
        from_addresses.push(tx.from.as_bytes());
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
    let df = df!(
        "block_number" => block_numbers,
        "transaction_index" => transaction_indices,
        "hash" => hashes,
        "nonce" => nonces,
        "from_addresses" => from_addresses,
        "to_addresses" => to_addresses,
        "value" => values,
        "inputs" => inputs,
        "gas" => gas,
        "gas_price" => gas_price,
        "transaction_type" => transaction_type,
        "max_priority_fee_per_gas" => max_priority_fee_per_gas,
        "max_fee_per_gas" => max_fee_per_gas,
        "chain_id" => chain_ids,
    );
    Ok(df?)
}

fn df_to_parquet(df: &mut DataFrame, filename: &str) {
    /// write polars dataframe to parquet file
    let file = std::fs::File::create(filename).unwrap();
    ParquetWriter::new(file)
        .with_statistics(true)
        .finish(df)
        .unwrap();
}
