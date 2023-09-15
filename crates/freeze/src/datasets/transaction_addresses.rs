use crate::{types::TransactionAddresses, ColumnType, Dataset, Datatype};
use std::collections::HashMap;

use ethers::prelude::*;
use polars::prelude::*;
use tokio::{sync::mpsc, task};

use crate::{
    dataframes::SortableDataFrame,
    types::{
        conversions::ToVecHex, BlockChunk, CollectError, RowFilter, Source, Table, TransactionChunk,
    },
    with_series, with_series_binary,
};

lazy_static::lazy_static! {
    pub static ref ERC20_TRANSFER: H256 = H256(
        prefix_hex::decode("0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef")
            .expect("Decoding failed"),
    );
}

#[async_trait::async_trait]
impl Dataset for TransactionAddresses {
    fn datatype(&self) -> Datatype {
        Datatype::TransactionAddresses
    }

    fn name(&self) -> &'static str {
        "transaction_addresses"
    }

    fn column_types(&self) -> HashMap<&'static str, ColumnType> {
        HashMap::from_iter(vec![
            ("block_number", ColumnType::UInt32),
            ("transaction_hash", ColumnType::Binary),
            ("address", ColumnType::Binary),
            ("relationship", ColumnType::String),
            ("chain_id", ColumnType::UInt64),
        ])
    }

    fn default_columns(&self) -> Vec<&'static str> {
        vec!["block_number", "transaction_hash", "address", "relationship", "chain_id"]
    }

    fn default_sort(&self) -> Vec<String> {
        vec![
            "block_number".to_string(),
            "transaction_hash".to_string(),
            "address".to_string(),
            "relationship".to_string(),
        ]
    }

    async fn collect_block_chunk(
        &self,
        chunk: &BlockChunk,
        source: &Source,
        schema: &Table,
        _filter: Option<&RowFilter>,
    ) -> Result<DataFrame, CollectError> {
        let rx = fetch_block_tx_addresses(chunk, source).await;
        traces_to_addresses_df(rx, schema, source.chain_id).await
    }

    async fn collect_transaction_chunk(
        &self,
        chunk: &TransactionChunk,
        source: &Source,
        schema: &Table,
        _filter: Option<&RowFilter>,
    ) -> Result<DataFrame, CollectError> {
        let rx = fetch_transaction_tx_addresses(chunk, source).await;
        traces_to_addresses_df(rx, schema, source.chain_id).await
    }
}

type BlockLogTraces = (Block<TxHash>, Vec<Log>, Vec<Trace>);

pub(crate) async fn fetch_block_tx_addresses(
    block_chunk: &BlockChunk,
    source: &Source,
) -> mpsc::Receiver<Result<BlockLogTraces, CollectError>> {
    let (tx, rx) = mpsc::channel(block_chunk.numbers().len());

    for number in block_chunk.numbers() {
        let tx = tx.clone();
        let source = source.clone();
        task::spawn(async move {
            let result = get_block_block_logs_traces(number, &source).await;
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

async fn fetch_transaction_tx_addresses(
    transaction_chunk: &TransactionChunk,
    source: &Source,
) -> mpsc::Receiver<Result<BlockLogTraces, CollectError>> {
    match transaction_chunk {
        TransactionChunk::Values(tx_hashes) => {
            let (tx, rx) = mpsc::channel(tx_hashes.len());
            for tx_hash in tx_hashes.iter() {
                let tx_hash = H256::from_slice(&tx_hash.clone());
                let tx = tx.clone();
                let source = source.clone();
                task::spawn(async move {
                    let result = get_tx_block_logs_traces(tx_hash, &source).await;
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

async fn get_block_block_logs_traces(
    number: u64,
    source: &Source,
) -> Result<BlockLogTraces, CollectError> {
    let block_number: BlockNumber = number.into();

    // block
    let block_result = source
        .fetcher
        .get_block(number)
        .await?
        .ok_or(CollectError::CollectError("could not get block data".to_string()))?;

    // logs
    let filter = Filter {
        block_option: FilterBlockOption::Range {
            from_block: Some(block_number),
            to_block: Some(block_number),
        },
        ..Default::default()
    };
    let log_result = source.fetcher.get_logs(&filter).await?;

    // traces
    let traces_result = source.fetcher.trace_block(block_number).await?;

    Ok((block_result, log_result, traces_result))
}

async fn get_tx_block_logs_traces(
    tx_hash: H256,
    source: &Source,
) -> Result<BlockLogTraces, CollectError> {
    let tx_data =
        source.fetcher.get_transaction(tx_hash).await?.ok_or_else(|| {
            CollectError::CollectError("could not find transaction data".to_string())
        })?;

    // block
    let block_number = tx_data
        .block_number
        .ok_or_else(|| CollectError::CollectError("block not found".to_string()))?
        .as_u64();
    let block_result = source
        .fetcher
        .get_block(block_number)
        .await?
        .ok_or(CollectError::CollectError("could not get block".to_string()))?;

    // logs
    let log_result = source
        .fetcher
        .get_transaction_receipt(tx_hash)
        .await?
        .ok_or(CollectError::CollectError("could not get tx receipt".to_string()))?
        .logs;

    // traces
    let traces_result = source.fetcher.trace_transaction(tx_hash).await?;

    Ok((block_result, log_result, traces_result))
}

async fn traces_to_addresses_df(
    mut rx: mpsc::Receiver<Result<BlockLogTraces, CollectError>>,
    schema: &Table,
    chain_id: u64,
) -> Result<DataFrame, CollectError> {
    // initialize
    let mut columns = TransactionAddressColumns::default();

    // parse stream of blocks
    while let Some(message) = rx.recv().await {
        match message {
            Ok((block, logs, traces)) => columns.process_tx_addresses(block, logs, traces, schema),
            Err(e) => {
                println!("{:?}", e);
                return Err(CollectError::TooManyRequestsError)
            }
        }
    }

    // convert to dataframes
    columns.create_df(schema, chain_id)
}

#[derive(Default)]
struct TransactionAddressColumns {
    n_rows: usize,
    block_number: Vec<u32>,
    transaction_hash: Vec<Vec<u8>>,
    address: Vec<Vec<u8>>,
    relationship: Vec<String>,
}

impl TransactionAddressColumns {
    fn process_tx_addresses(
        &mut self,
        block: Block<TxHash>,
        logs: Vec<Log>,
        traces: Vec<Trace>,
        schema: &Table,
    ) {
        let mut logs_by_tx: HashMap<H256, Vec<Log>> = HashMap::new();
        for log in logs.into_iter() {
            if let Some(tx_hash) = log.transaction_hash {
                logs_by_tx.entry(tx_hash).or_insert_with(Vec::new).push(log);
            }
        }

        let (block_number, block_author) = match (block.number, block.author) {
            (Some(number), Some(author)) => (number.as_u64(), author),
            _ => return,
        };

        let mut current_tx_hash = H256([0; 32]);
        for trace in traces.iter() {
            if let (Some(tx_hash), Some(_tx_pos)) =
                (trace.transaction_hash, trace.transaction_position)
            {
                let first_trace_in_tx = tx_hash != current_tx_hash;

                if first_trace_in_tx {
                    self.process_address(
                        block_author,
                        "miner_fee",
                        trace.block_number,
                        tx_hash,
                        schema,
                    );

                    // erc transfers
                    if let Some(logs) = logs_by_tx.get(&tx_hash) {
                        for log in logs.iter() {
                            if log.topics.len() >= 3 {
                                let event = log.topics[0];
                                let name = if event == *ERC20_TRANSFER {
                                    if log.data.len() > 0 {
                                        Some("erc20_transfer")
                                    } else if log.topics.len() == 4 {
                                        Some("erc721_transfer")
                                    } else {
                                        None
                                    }
                                } else {
                                    None
                                };
                                if let Some(name) = name {
                                    let mut from: [u8; 20] = [0; 20];
                                    from.copy_from_slice(&log.topics[1].to_fixed_bytes()[12..32]);
                                    self.process_address(
                                        H160(from),
                                        &(name.to_string() + "_from"),
                                        block_number,
                                        tx_hash,
                                        schema,
                                    );

                                    let mut to: [u8; 20] = [0; 20];
                                    to.copy_from_slice(&log.topics[1].to_fixed_bytes()[12..32]);
                                    self.process_address(
                                        H160(to),
                                        &(name.to_string() + "_to"),
                                        block_number,
                                        tx_hash,
                                        schema,
                                    );
                                }
                            }
                        }
                    }

                    match &trace.action {
                        Action::Call(action) => {
                            self.process_address(
                                action.from,
                                "tx_from",
                                trace.block_number,
                                tx_hash,
                                schema,
                            );
                            self.process_address(
                                action.to,
                                "tx_to",
                                trace.block_number,
                                tx_hash,
                                schema,
                            );
                        }
                        Action::Create(action) => {
                            self.process_address(
                                action.from,
                                "tx_from",
                                trace.block_number,
                                tx_hash,
                                schema,
                            );
                        }
                        _ => panic!("invalid first tx trace"),
                    }

                    if let Some(Res::Create(result)) = &trace.result {
                        self.process_address(
                            result.address,
                            "tx_to",
                            trace.block_number,
                            tx_hash,
                            schema,
                        );
                    };
                }

                match &trace.action {
                    Action::Call(action) => {
                        // let (from_name, to_name) = if first_trace_in_tx {
                        //     ("tx_from", "tx_to")
                        // } else {
                        //     ("call_from", "call_to")
                        // };
                        self.process_address(
                            action.from,
                            "call_from",
                            trace.block_number,
                            tx_hash,
                            schema,
                        );
                        self.process_address(
                            action.to,
                            "call_to",
                            trace.block_number,
                            tx_hash,
                            schema,
                        );
                    }
                    Action::Create(action) => {
                        self.process_address(
                            action.from,
                            "factory",
                            trace.block_number,
                            tx_hash,
                            schema,
                        );
                    }
                    Action::Suicide(action) => {
                        self.process_address(
                            action.address,
                            "suicide",
                            trace.block_number,
                            tx_hash,
                            schema,
                        );
                        self.process_address(
                            action.refund_address,
                            "suicide_refund",
                            trace.block_number,
                            tx_hash,
                            schema,
                        );
                    }
                    Action::Reward(action) => {
                        self.process_address(
                            action.author,
                            "author",
                            trace.block_number,
                            tx_hash,
                            schema,
                        );
                    }
                }

                if let Some(Res::Create(result)) = &trace.result {
                    self.process_address(
                        result.address,
                        "create",
                        trace.block_number,
                        tx_hash,
                        schema,
                    );
                };

                current_tx_hash = tx_hash;
            }
        }
    }

    fn process_address(
        &mut self,
        address: H160,
        relationship: &str,
        block_number: u64,
        transaction_hash: H256,
        schema: &Table,
    ) {
        self.n_rows += 1;
        if schema.has_column("address") {
            self.address.push(address.as_bytes().to_vec());
        }
        if schema.has_column("relationship") {
            self.relationship.push(relationship.to_string());
        }
        if schema.has_column("block_number") {
            self.block_number.push(block_number as u32);
        }
        if schema.has_column("transaction_hash") {
            self.transaction_hash.push(transaction_hash.as_bytes().to_vec());
        }
    }

    fn create_df(self, schema: &Table, chain_id: u64) -> Result<DataFrame, CollectError> {
        let mut cols = Vec::with_capacity(schema.columns().len());
        with_series!(cols, "block_number", self.block_number, schema);
        with_series_binary!(cols, "transaction_hash", self.transaction_hash, schema);
        with_series_binary!(cols, "address", self.address, schema);
        with_series!(cols, "relationship", self.relationship, schema);
        with_series!(cols, "chain_id", vec![chain_id; self.n_rows], schema);

        DataFrame::new(cols).map_err(CollectError::PolarsError).sort_by_schema(schema)
    }
}
