use std::{collections::HashMap, sync::Arc};

use ethers::prelude::*;
use ethers_core::abi::{AbiEncode, HumanReadableParser, RawLog, Token};
use polars::export::ahash::HashSet;
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


    let decoder = match schema.clone().meta {
        Some(tm) => tm.log_decoder,
        None => None,
    };

    let mut event_cols: HashMap<String, Vec<Token>> = HashMap::new();

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
                if let Some(decoder) = decoder.clone() {
                    decoder.parse_log_from_event(logs).into_iter().for_each(|(k, v)| {
                        event_cols.entry(k).or_insert(Vec::new()).extend(v);
                    });
                }
            }
            _ => return Err(CollectError::TooManyRequestsError),
        }
    }

    let mut cols = Vec::new();
    with_series!(cols, "block_number", block_number.clone(), schema);
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

    if let Some(decoder) = decoder {
        // Write columns even if there are no values decoded - indicates empty dataframe
        let chunk_len = block_number.len();
        if event_cols.is_empty() {
            for name in decoder.field_names().iter() {
                cols.push(Series::new(name.as_str(), vec![None::<u64>; chunk_len]));
            }
        } else {
            for (name, data) in event_cols {
                match LogDecoder::make_series(name.clone(), data, chunk_len.clone()) {
                    Ok(s) => {
                        println!("Pushing col {:?}", name.clone());
                        cols.push(s);
                    }
                    Err(e) => eprintln!("error creating frame: {}", e), // TODO: see how best to bubble up error
                }
            }
        }
    }


    DataFrame::new(cols).map_err(CollectError::PolarsError).sort_by_schema(schema)
}


#[derive(Clone, Debug, PartialEq)]
pub struct LogDecoder {
    pub raw: String,
    pub event: abi::Event,
}

impl LogDecoder {
    /// create a new LogDecoder from an event signature
    /// ex: LogDecoder::new("event Transfer(address indexed from, address indexed to, uint256 amount)".to_string())
    pub fn new(event_signature: String) -> Option<Self> {
        match HumanReadableParser::parse_event(event_signature.as_str())
        {
            Ok(event) => Some(Self { event, raw: event_signature.clone() }),
            Err(_) => {
                eprintln!("incorrectly formatted event {} (expect something like event Transfer(address indexed from, address indexed to, uint256 amount)", event_signature);
                None
            }
        }
    }

    fn field_names(&self) -> Vec<String> {
        self.event.inputs.iter().map(|i| i.name.clone()).collect()
    }

    /// converts from a log type to an abi token type
    /// this function assumes all logs are of the same type and skips fields if they don't match the passed event definition
    pub fn parse_log_from_event(&self, logs: Vec<Log>) -> HashMap<String, Vec<Token>> {
        let mut map: HashMap<String, Vec<Token>> = HashMap::new();
        let known_keys = self.event.inputs.clone().into_iter().map(|i| i.name).collect::<HashSet<String>>();

        for log in logs {
            if let Ok(log) = self.event.parse_log(RawLog::from(log)) {
                for param in log.params {
                    if known_keys.contains(param.name.as_str()) {
                        let tokens = map.entry(param.name).or_insert(Vec::new());
                        tokens.push(param.value);
                    }
                }
            }
        }
        map
    }

    /// data should never be mixed type, otherwise this will return inconsistent results
    pub fn make_series(name: String, data: Vec<Token>, chunk_len: usize) -> Result<Series, String> {

        // This is a smooth brain way of doing this, but I can't think of a better way right now
        let mut ints: Vec<u64> = vec![];
        let mut str_ints: Vec<String> = vec![];
        let mut bytes: Vec<String> = vec![];
        let mut bools: Vec<bool> = vec![];
        let mut strings: Vec<String> = vec![];
        let mut addresses: Vec<String> = vec![];
        // TODO: support array & tuple types

        for token in data.clone() {
            match token {
                Token::Address(a) => addresses.push(format!("{:?}", a)),
                Token::FixedBytes(b) => bytes.push(b.encode_hex()),
                Token::Bytes(b) => bytes.push(b.encode_hex()),
                // LogParam and Token both don't specify the size of the int, so we have to guess.
                // try to cast the all to u64, if that fails store as string and collect the ones that
                // succeed at the end.
                // this may get problematic if 1 batch of logs happens to contain all u64-able ints and
                // the next batch contains u256s. Might be worth just casting all as strings
                Token::Int(i) | Token::Uint(i) => match i.try_into() {
                    Ok(i) => ints.push(i),
                    Err(_) => str_ints.push(i.to_string()),
                },
                Token::Bool(b) => bools.push(b),
                Token::String(s) => strings.push(s),
                Token::Array(_) | Token::FixedArray(_) => {}
                Token::Tuple(_) => {}
            }
        }
        let mixed_length_err = format!("could not parse column {}, mixed type", name);


        // check each vector, see if it contains any values, if it does, check if it's the same length
        // as the input data and map to a series
        if ints.len() > 0 || str_ints.len() > 0 {
            if str_ints.len() > 0 {
                str_ints.extend(ints.into_iter().map(|i| i.to_string()));
                if str_ints.len() != chunk_len {
                    return Err(mixed_length_err);
                }
                return Ok(Series::new(name.as_str(), str_ints));
            }
            Ok(Series::new(name.as_str(), ints))
        } else if bytes.len() > 0 {
            if bytes.len() != chunk_len {
                return Err(mixed_length_err);
            }
            Ok(Series::new(name.as_str(), bytes))
        } else if bools.len() > 0 {
            if bools.len() != chunk_len {
                return Err(mixed_length_err);
            }
            Ok(Series::new(name.as_str(), bools))
        } else if strings.len() > 0 {
            if strings.len() != chunk_len {
                return Err(mixed_length_err);
            }
            Ok(Series::new(name.as_str(), strings))
        } else if addresses.len() > 0 {
            if addresses.len() != chunk_len {
                return Err(mixed_length_err);
            }
            Ok(Series::new(name.as_str(), addresses))
        } else {
            // case where no data was passed
            Ok(Series::new(name.as_str(), vec![None::<u64>; chunk_len]))
        }
    }
}

#[cfg(test)]
mod test {
    use polars::prelude::DataType::Boolean;
    use super::*;

    #[test]
    fn test_mapping_log_into_type_columns() {
        let raw = "event NewMint(address indexed msgSender, uint256 indexed mintQuantity)";
        let e = HumanReadableParser::parse_event(raw).unwrap();

        let raw_log = r#"{
            "address": "0x0000000000000000000000000000000000000000",
            "topics": [
                "0x52277f0b4a9b555c5aa96900a13546f972bda413737ec164aac947c87eec6024",
                "0x00000000000000000000000062a73d9116eda78a78f4cf81602bdc926fb4c0dd",
                "0x0000000000000000000000000000000000000000000000000000000000000003"
            ],
            "data": "0x"
        }"#;

        let decoder = LogDecoder { raw: raw.to_string(), event: e.clone() };

        let log = serde_json::from_str::<Log>(raw_log).unwrap();
        let m = decoder.parse_log_from_event(vec![log]);
        assert_eq!(m.len(), 2);
        assert_eq!(m.get("msgSender").unwrap().len(), 1);
        assert_eq!(m.get("mintQuantity").unwrap().len(), 1);
    }

    #[test]
    fn test_parsing_bools() {
        let s = LogDecoder::make_series("bools".to_string(), vec![Token::Bool(true), Token::Bool(false)]).unwrap();
        assert_eq!(s.dtype(), &Boolean);
        assert_eq!(s.len(), 2)
    }

    #[test]
    fn test_parsing_ints() {
        let s = LogDecoder::make_series("ints".to_string(), vec![Token::Int(1.into()), Token::Int(2.into())]).unwrap();
        assert_eq!(s.dtype(), &DataType::UInt64);
        assert_eq!(s.len(), 2)
    }

    #[test]
    fn test_parsing_big_ints() {
        let s = LogDecoder::make_series("ints".to_string(), vec![Token::Int(U256::max_value()), Token::Int(2.into())]).unwrap();
        assert_eq!(s.dtype(), &DataType::Utf8);
        assert_eq!(s.len(), 2)
    }

    #[test]
    fn test_parsing_addresses() {
        let s = LogDecoder::make_series("ints".to_string(), vec![Token::Address(Address::zero()), Token::Address(Address::zero())]).unwrap();
        assert_eq!(s.dtype(), &DataType::Utf8);
        assert_eq!(s.len(), 2)
    }
}