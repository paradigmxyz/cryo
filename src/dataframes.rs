use ethers::prelude::*;
use polars::prelude::*;
use crate::types::SlimBlock;

/// write polars dataframe to file
pub fn df_to_file(df: &mut DataFrame, filename: &str) {
    match filename {
        _ if filename.ends_with(".parquet") => df_to_parquet(df, filename),
        _ if filename.ends_with(".csv") => df_to_csv(df, filename),
        _ => panic!("invalid file format")
    }
}

/// write polars dataframe to parquet file
pub fn df_to_parquet(df: &mut DataFrame, filename: &str) {
    let file = std::fs::File::create(filename).unwrap();
    ParquetWriter::new(file)
        .with_statistics(true)
        .finish(df)
        .unwrap();
}

/// write polars dataframe to csv file
pub fn df_to_csv(df: &mut DataFrame, filename: &str) {
    let file = std::fs::File::create(filename).unwrap();
    CsvWriter::new(file)
        .finish(df)
        .unwrap();
}

//pub fn df_binary_columns_to_hex(df: &DataFrame) -> Result<DataFrame, PolarsError> {
//    // let mut binary_columns: Vec<Series> = vec![];
//    // let mut binary_columns: Vec<ChunkedArray<Utf8Type>> = vec![];
//    // let mut binary_exprs: Vec<Expr> = vec![];
//    let columns: &[Series] = &df.get_columns();
//    let mut lazy = df.clone().lazy();
//    // let mut df_hex = df.clone();
//    columns.iter().for_each(|column| {
//        match column.dtype() {
//            polars::datatypes::DataType::Binary => {
//                // binary_columns.push(column.binary().unwrap().encode("hex"));
//                // binary_columns.push(column.binary().unwrap().encode("hex"));
//                // let encoded = column.utf8().unwrap().hex_encode();
//                // let encoded = col("hi").binary().encode();
//                // binary_columns.push(encoded.into_series());
//                // binary_exprs.push(column.encode_hex());
//                // df_hex = df_hex.clone().with_column(encoded).unwrap().clone();
//                //
//                let expr = col(column.name()).bin().encode("hex");
//                lazy = lazy.clone().with_column(expr);
//            },
//            _ => ()
//        }
//    });
//    lazy.collect()
//    // Ok(df_hex)
//}

// pub fn df_binary_columns_to_hex(df: &DataFrame) -> Result<DataFrame, PolarsError> {
//     let columns: &[Series] = &df.get_columns();
//     let mut lazy = df.clone().lazy();
//     columns.iter().for_each(|column| {
//         if column.dtype() == &polars::datatypes::DataType::Binary {
//             let expr = lit("0x") + col(column.name()).binary().encode("hex");
//             lazy = lazy.clone().with_column(expr);
//         }
//     });
//     lazy.collect()
// }

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
    let mut log_type: Vec<Option<String>> = Vec::new();

    for log in logs.iter() {
        if log.removed.unwrap() { continue };

        address.push(log.address.as_bytes().to_vec());
        match log.topics.len() {
            0 => {
                topic0.push(None);
                topic1.push(None);
                topic2.push(None);
                topic3.push(None);
            },
            1 => {
                topic0.push(Some(log.topics[0].as_bytes().to_vec()));
                topic1.push(None);
                topic2.push(None);
                topic3.push(None);
            },
            2 => {
                topic0.push(Some(log.topics[0].as_bytes().to_vec()));
                topic1.push(Some(log.topics[1].as_bytes().to_vec()));
                topic2.push(None);
                topic3.push(None);
            },
            3 => {
                topic0.push(Some(log.topics[0].as_bytes().to_vec()));
                topic1.push(Some(log.topics[1].as_bytes().to_vec()));
                topic2.push(Some(log.topics[2].as_bytes().to_vec()));
                topic3.push(None);
            },
            4 => {
                topic0.push(Some(log.topics[0].as_bytes().to_vec()));
                topic1.push(Some(log.topics[1].as_bytes().to_vec()));
                topic2.push(Some(log.topics[2].as_bytes().to_vec()));
                topic3.push(Some(log.topics[3].as_bytes().to_vec()));
            },
            _ => { panic!("Invalid number of topics"); }
        }
        data.push(log.data.clone().to_vec());
        block_number.push(log.block_number.unwrap().as_u64());
        transaction_hash.push(log.transaction_hash.map(|hash| hash.as_bytes().to_vec()).unwrap());
        transaction_index.push(log.transaction_index.unwrap().as_u64());
        log_index.push(log.log_index.unwrap().as_u64());
        log_type.push(log.log_type.clone());
    }

    let df = df!(
        "address" => address,
        "topic0" => topic0,
        "topic1" => topic1,
        "topic2" => topic2,
        "topic3" => topic3,
        "data" => data,
        "block_number" => block_number,
        "transaction_hash" => transaction_hash,
        "transaction_index" => transaction_index,
        "log_index" => log_index,
        "log_type" => log_type,
    );

    Ok(df?)
}
