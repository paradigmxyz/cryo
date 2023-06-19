use ethers::prelude::*;
use polars::prelude::*;
use crate::gather::SlimBlock;

/// write polars dataframe to parquet file
pub fn df_to_parquet(df: &mut DataFrame, filename: &str) {
    let file = std::fs::File::create(filename).unwrap();
    ParquetWriter::new(file)
        .with_statistics(true)
        .finish(df)
        .unwrap();
}


pub fn preview_df(df: &DataFrame, name: &str) {
    println!("{}", name);
    println!("{:?}", df);
    println!("{:?}", df.schema());
}


pub fn blocks_to_df(blocks: Vec<SlimBlock>) -> Result<DataFrame, Box<dyn std::error::Error>> {
    let mut number: Vec<u64> = Vec::new();
    let mut hash: Vec<Vec<u8>> = Vec::new();
    let mut author: Vec<Vec<u8>> = Vec::new();
    let mut gas_used: Vec<u64> = Vec::new();
    let mut extra_data: Vec<Vec<u8>> = Vec::new();
    let mut timestamp: Vec<u64> = Vec::new();
    let mut base_fee_per_gas: Vec<Option<u64>> = Vec::new();

    for block in blocks.iter() {
        number.push(block.number);
        hash.push(block.hash.clone());
        author.push(block.author.clone());
        gas_used.push(block.gas_used);
        extra_data.push(block.extra_data.clone());
        timestamp.push(block.timestamp);
        base_fee_per_gas.push(block.base_fee_per_gas);
    }

    let df = df!(
        "block_number" => number,
        "block_hash" => hash,
        "author" => author,
        "gas_used" => gas_used,
        "extra_data" => extra_data,
        "timestamp" => timestamp,
        "base_fee_per_gas" => base_fee_per_gas,
    );

    Ok(df?)
}

/// convert a Vec<Transaction> into polars dataframe
pub fn txs_to_df(txs: Vec<Transaction>) -> Result<DataFrame, Box<dyn std::error::Error>> {
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
