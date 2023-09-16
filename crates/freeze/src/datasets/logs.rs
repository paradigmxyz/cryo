use std::collections::{HashMap, HashSet};

use ethers::prelude::*;
use ethers_core::abi::{AbiEncode, EventParam, HumanReadableParser, ParamType, RawLog, Token};
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

pub(crate) async fn fetch_block_logs(
    block_chunk: &BlockChunk,
    source: &Source,
    filter: Option<&RowFilter>,
) -> mpsc::Receiver<Result<Vec<Log>, CollectError>> {
    // todo: need to modify these functions so they turn a result
    let request_chunks = block_chunk.to_log_filter_options(&source.inner_request_size);
    let (tx, rx) = mpsc::channel(request_chunks.len());
    for request_chunk in request_chunks.iter() {
        let tx = tx.clone();
        let fetcher = source.fetcher.clone();
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
            let result = fetcher.get_logs(&log_filter).await;
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

pub(crate) async fn fetch_transaction_logs(
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
                let fetcher = source.fetcher.clone();
                task::spawn(async move {
                    let receipt = fetcher.get_transaction_receipt(H256::from_slice(&tx_hash)).await;
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
    event_cols: HashMap<String, Vec<Token>>,
}

impl LogColumns {
    pub(crate) fn process_logs(
        &mut self,
        logs: Vec<Log>,
        schema: &Table,
    ) -> Result<(), CollectError> {
        for log in &logs {
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

        // add decoded event logs
        let decoder = schema.log_decoder.clone();
        if let Some(decoder) = decoder {
            decoder.parse_log_from_event(logs).into_iter().for_each(|(k, v)| {
                self.event_cols.entry(k).or_insert(Vec::new()).extend(v);
            });
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

        let decoder = schema.log_decoder.clone();
        if let Some(decoder) = decoder {
            // Write columns even if there are no values decoded - indicates empty dataframe
            let chunk_len = self.n_rows;
            if self.event_cols.is_empty() {
                for name in decoder.field_names().iter() {
                    cols.push(Series::new(name.as_str(), vec![None::<u64>; chunk_len]));
                }
            } else {
                for (name, data) in self.event_cols {
                    match decoder.make_series(name.clone(), data, chunk_len) {
                        Ok(s) => {
                            cols.push(s);
                        }
                        Err(e) => eprintln!("error creating frame: {}", e), /* TODO: see how best
                                                                             * to
                                                                             * bubble up error */
                    }
                }
            }
        }

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

/// container for log decoding context
#[derive(Clone, Debug, PartialEq)]
pub struct LogDecoder {
    /// the raw event signature string ex: event Transfer(address indexed from, address indexed to,
    /// uint256 amount)
    pub raw: String,
    /// decoded abi type of event signature string
    pub event: abi::Event,
}

impl LogDecoder {
    /// create a new LogDecoder from an event signature
    /// ex: LogDecoder::new("event Transfer(address indexed from, address indexed to, uint256
    /// amount)".to_string())
    pub fn new(event_signature: String) -> Result<Self, String> {
        match HumanReadableParser::parse_event(event_signature.as_str()) {
            Ok(event) => Ok(Self { event, raw: event_signature.clone() }),
            Err(e) => {
                let err = format!("incorrectly formatted event {} (expect something like event Transfer(address indexed from, address indexed to, uint256 amount) err: {}", event_signature, e);
                eprintln!("{}", err);
                Err(err)
            }
        }
    }

    fn field_names(&self) -> Vec<String> {
        self.event.inputs.iter().map(|i| i.name.clone()).collect()
    }

    /// converts from a log type to an abi token type
    /// this function assumes all logs are of the same type and skips fields if they don't match the
    /// passed event definition
    pub fn parse_log_from_event(&self, logs: Vec<Log>) -> HashMap<String, Vec<Token>> {
        let mut map: HashMap<String, Vec<Token>> = HashMap::new();
        let known_keys =
            self.event.inputs.clone().into_iter().map(|i| i.name).collect::<HashSet<String>>();

        for log in logs {
            match self.event.parse_log(RawLog::from(log)) {
                Ok(log) => {
                    for param in log.params {
                        if known_keys.contains(param.name.as_str()) {
                            let tokens = map.entry(param.name).or_insert(Vec::new());
                            tokens.push(param.value);
                        }
                    }
                }
                Err(e) => eprintln!("error parsing log: {:?}", e),
            }
        }
        map
    }

    /// data should never be mixed type, otherwise this will return inconsistent results
    pub fn make_series(
        &self,
        name: String,
        data: Vec<Token>,
        chunk_len: usize,
    ) -> Result<Series, String> {
        // This is a smooth brain way of doing this, but I can't think of a better way right now
        let mut ints: Vec<i64> = vec![];
        let mut uints: Vec<u64> = vec![];
        let mut str_ints: Vec<String> = vec![];
        let mut bytes: Vec<String> = vec![];
        let mut bools: Vec<bool> = vec![];
        let mut strings: Vec<String> = vec![];
        let mut addresses: Vec<String> = vec![];
        // TODO: support array & tuple types

        let param = self
            .event
            .inputs
            .clone()
            .into_iter()
            .filter(|i| i.name == name)
            .collect::<Vec<EventParam>>();
        let param = param.first();

        for token in data {
            match token {
                Token::Address(a) => addresses.push(format!("{:?}", a)),
                Token::FixedBytes(b) => bytes.push(b.encode_hex()),
                Token::Bytes(b) => bytes.push(b.encode_hex()),
                Token::Uint(i) => match param {
                    Some(param) => {
                        match param.kind.clone() {
                            ParamType::Uint(size) => {
                                if size <= 64 {
                                    uints.push(i.as_u64())
                                } else {
                                    // TODO: decode this based on U256Types flag
                                    str_ints.push(i.to_string())
                                }
                            }
                            _ => str_ints.push(i.to_string()),
                        }
                    }
                    None => match i.try_into() {
                        Ok(i) => ints.push(i),
                        Err(_) => str_ints.push(i.to_string()),
                    },
                },
                Token::Int(i) => match param {
                    Some(param) => {
                        match param.kind.clone() {
                            ParamType::Int(size) => {
                                if size <= 64 {
                                    match i.as_u64().try_into() {
                                        Ok(i) => ints.push(i),
                                        Err(_) => str_ints.push(i.to_string()),
                                    }
                                } else {
                                    // TODO: decode this based on U256Types flag
                                    str_ints.push(i.to_string())
                                }
                            }
                            _ => str_ints.push(i.to_string()),
                        }
                    }
                    None => match i.try_into() {
                        Ok(i) => ints.push(i),
                        Err(_) => str_ints.push(i.to_string()),
                    },
                },
                Token::Bool(b) => bools.push(b),
                Token::String(s) => strings.push(s),
                Token::Array(_) | Token::FixedArray(_) => {}
                Token::Tuple(_) => {}
            }
        }
        let mixed_length_err = format!("could not parse column {}, mixed type", name);

        // check each vector, see if it contains any values, if it does, check if it's the same
        // length as the input data and map to a series
        if !ints.is_empty() {
            Ok(Series::new(name.as_str(), ints))
        } else if !uints.is_empty() {
            Ok(Series::new(name.as_str(), uints))
        } else if !str_ints.is_empty() {
            Ok(Series::new(name.as_str(), str_ints))
        } else if !bytes.is_empty() {
            if bytes.len() != chunk_len {
                return Err(mixed_length_err)
            }
            Ok(Series::new(name.as_str(), bytes))
        } else if !bools.is_empty() {
            if bools.len() != chunk_len {
                return Err(mixed_length_err)
            }
            Ok(Series::new(name.as_str(), bools))
        } else if !strings.is_empty() {
            if strings.len() != chunk_len {
                return Err(mixed_length_err)
            }
            Ok(Series::new(name.as_str(), strings))
        } else if !addresses.is_empty() {
            if addresses.len() != chunk_len {
                return Err(mixed_length_err)
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
    use super::*;
    use polars::prelude::DataType::Boolean;

    const RAW: &str = "event NewMint(address indexed msgSender, uint256 indexed mintQuantity)";
    const RAW_LOG: &str = r#"{
            "address": "0x0000000000000000000000000000000000000000",
            "topics": [
                "0x52277f0b4a9b555c5aa96900a13546f972bda413737ec164aac947c87eec6024",
                "0x00000000000000000000000062a73d9116eda78a78f4cf81602bdc926fb4c0dd",
                "0x0000000000000000000000000000000000000000000000000000000000000003"
            ],
            "data": "0x"
        }"#;

    fn make_log_decoder() -> LogDecoder {
        let e = HumanReadableParser::parse_event(RAW).unwrap();

        LogDecoder { raw: RAW.to_string(), event: e.clone() }
    }

    #[test]
    fn test_mapping_log_into_type_columns() {
        let decoder = make_log_decoder();
        let log = serde_json::from_str::<Log>(RAW_LOG).unwrap();
        let m = decoder.parse_log_from_event(vec![log]);
        assert_eq!(m.len(), 2);
        assert_eq!(m.get("msgSender").unwrap().len(), 1);
        assert_eq!(m.get("mintQuantity").unwrap().len(), 1);
    }

    #[test]
    fn test_parsing_bools() {
        let s = make_log_decoder()
            .make_series("bools".to_string(), vec![Token::Bool(true), Token::Bool(false)], 2)
            .unwrap();
        assert_eq!(s.dtype(), &Boolean);
        assert_eq!(s.len(), 2)
    }

    #[test]
    fn test_parsing_ints_uint256() {
        let s = make_log_decoder()
            .make_series(
                "mintQuantity".to_string(),
                vec![Token::Uint(1.into()), Token::Uint(2.into())],
                2,
            )
            .unwrap();
        assert_eq!(s.dtype(), &DataType::Utf8);
        assert_eq!(s.len(), 2)
    }

    #[test]
    fn test_parsing_ints_uint64() {
        let raw = "event NewMint(address indexed msgSender, uint64 indexed mintQuantity)";

        // let e = HumanReadableParser::parse_event(raw).unwrap();
        let decoder = LogDecoder::new(raw.to_string()).unwrap();

        let s = decoder
            .make_series(
                "mintQuantity".to_string(),
                vec![Token::Uint(1.into()), Token::Uint(2.into())],
                2,
            )
            .unwrap();
        assert_eq!(s.dtype(), &DataType::UInt64);
        assert_eq!(s.len(), 2)
    }

    #[test]
    fn test_parsing_big_ints() {
        let s = make_log_decoder()
            .make_series(
                "msgSender".to_string(),
                vec![Token::Int(U256::max_value()), Token::Int(2.into())],
                2,
            )
            .unwrap();
        assert_eq!(s.dtype(), &DataType::Utf8);
        assert_eq!(s.len(), 2)
    }

    #[test]
    fn test_parsing_addresses() {
        let s = make_log_decoder()
            .make_series(
                "ints".to_string(),
                vec![Token::Address(Address::zero()), Token::Address(Address::zero())],
                2,
            )
            .unwrap();
        assert_eq!(s.dtype(), &DataType::Utf8);
        assert_eq!(s.len(), 2)
    }
}
