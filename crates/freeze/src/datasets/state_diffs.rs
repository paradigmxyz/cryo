use std::collections::{HashMap, HashSet};

use ethers::{
    prelude::*,
    providers::{JsonRpcClient, ProviderError},
};
use polars::prelude::*;
use tokio::sync::mpsc;

use crate::{
    dataframes::SortableDataFrame,
    types::{
        conversions::ToVecHex, BlockChunk, ChunkData, CollectError, ColumnType, Datatype,
        MultiDataset, RowFilter, Source, StateDiffs, Table, TransactionChunk,
    },
    with_series, with_series_binary,
};

// entries: block_number, transaction_indices, transaction_traces
pub(crate) type BlockNumberTransactionsTraces =
    Result<(Option<u32>, Vec<(u32, BlockTrace)>), CollectError>;

#[async_trait::async_trait]
impl MultiDataset for StateDiffs {
    fn name(&self) -> &'static str {
        "state_diffs"
    }

    fn datatypes(&self) -> HashSet<Datatype> {
        [Datatype::BalanceDiffs, Datatype::CodeDiffs, Datatype::NonceDiffs, Datatype::StorageDiffs]
            .into_iter()
            .collect()
    }

    async fn collect_block_chunk(
        &self,
        chunk: &BlockChunk,
        source: &Source<Provider<impl JsonRpcClient>>,
        schemas: HashMap<Datatype, Table>,
        _filter: HashMap<Datatype, RowFilter>,
    ) -> Result<HashMap<Datatype, DataFrame>, CollectError> {
        let rx = fetch_block_state_diffs(chunk, source).await;
        state_diffs_to_df(rx, &schemas, source.chain_id).await
    }

    async fn collect_transaction_chunk(
        &self,
        chunk: &TransactionChunk,
        source: &Source<Provider<impl JsonRpcClient>>,
        schemas: HashMap<Datatype, Table>,
        _filter: HashMap<Datatype, RowFilter>,
    ) -> Result<HashMap<Datatype, DataFrame>, CollectError> {
        let include_indices = schemas.values().any(|schema| schema.has_column("block_number"));
        let rx = fetch_transaction_state_diffs(chunk, source, include_indices).await;
        state_diffs_to_df(rx, &schemas, source.chain_id).await
    }
}

pub(crate) async fn collect_block_state_diffs(
    datatype: &Datatype,
    chunk: &BlockChunk,
    source: &Source<Provider<impl JsonRpcClient>>,
    schema: &Table,
    _filter: Option<&RowFilter>,
) -> Result<DataFrame, CollectError> {
    let rx = fetch_block_state_diffs(chunk, source).await;
    let mut schemas: HashMap<Datatype, Table> = HashMap::new();
    schemas.insert(*datatype, schema.clone());
    let dfs = state_diffs_to_df(rx, &schemas, source.chain_id).await;

    // get single df out of result
    let df = match dfs {
        Ok(mut dfs) => match dfs.remove(datatype) {
            Some(df) => Ok(df),
            None => Err(CollectError::BadSchemaError),
        },
        Err(e) => Err(e),
    };

    df.sort_by_schema(schema)
}

pub(crate) async fn collect_transaction_state_diffs(
    datatype: &Datatype,
    chunk: &TransactionChunk,
    source: &Source<Provider<impl JsonRpcClient>>,
    schema: &Table,
    _filter: Option<&RowFilter>,
) -> Result<DataFrame, CollectError> {
    let include_indices = schema.has_column("block_number");
    let chain_id = source.chain_id;
    let rx = fetch_transaction_state_diffs(chunk, source, include_indices).await;
    let mut schemas: HashMap<Datatype, Table> = HashMap::new();
    schemas.insert(*datatype, schema.clone());
    let dfs = state_diffs_to_df(rx, &schemas, chain_id).await;

    // get single df out of result
    let df = match dfs {
        Ok(mut dfs) => match dfs.remove(datatype) {
            Some(df) => Ok(df),
            None => Err(CollectError::BadSchemaError),
        },
        Err(e) => Err(e),
    };

    df.sort_by_schema(schema)
}

pub(crate) async fn fetch_block_traces(
    block_chunk: &BlockChunk,
    trace_types: &[TraceType],
    source: &Source<Provider<impl JsonRpcClient>>,
) -> mpsc::Receiver<BlockNumberTransactionsTraces> {
    let (tx, rx) = mpsc::channel(block_chunk.size() as usize);
    for number in block_chunk.numbers() {
        if number == 0 {
            continue
        };
        let tx = tx.clone();
        let fetcher = source.fetcher.clone();
        let trace_types = trace_types.to_vec();
        tokio::spawn(async move {
            let result = fetcher
                .trace_replay_block_transactions(BlockNumber::Number(number.into()), trace_types)
                .await
                .map(|res| {
                    (
                        Some(number as u32),
                        res.into_iter()
                            .enumerate()
                            .map(|(index, traces)| (index as u32, traces))
                            .collect(),
                    )
                });
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

pub(crate) async fn fetch_transaction_traces(
    transaction_chunk: &TransactionChunk,
    trace_types: &[TraceType],
    source: &Source<Provider<impl JsonRpcClient>>,
    include_indices: bool,
) -> mpsc::Receiver<BlockNumberTransactionsTraces> {
    match transaction_chunk {
        TransactionChunk::Values(tx_hashes) => {
            let (tx, rx) = mpsc::channel(tx_hashes.len());
            for tx_hash in tx_hashes.iter() {
                let tx_hash = tx_hash.clone();
                let tx = tx.clone();
                let fetcher = source.fetcher.clone();
                let trace_types = trace_types.to_vec();
                tokio::spawn(async move {
                    let tx_hash = H256::from_slice(&tx_hash);
                    let result = fetcher.trace_replay_transaction(tx_hash, trace_types).await;
                    let result = match result {
                        Ok(trace) => {
                            let trace = BlockTrace { transaction_hash: Some(tx_hash), ..trace };
                            if include_indices {
                                match fetcher.get_transaction(tx_hash).await {
                                    Ok(Some(tx)) => match (tx.block_number, tx.transaction_index) {
                                        (Some(block_number), Some(tx_index)) => Ok((
                                            Some(block_number.as_u32()),
                                            vec![(tx_index.as_u32(), trace)],
                                        )),
                                        _ => Err(CollectError::CollectError(
                                            "could not get block number".to_string(),
                                        )),
                                    },
                                    _ => Err(CollectError::CollectError(
                                        "could not get block number".to_string(),
                                    )),
                                }
                            } else {
                                Ok((None, vec![(0, trace)]))
                            }
                        }
                        Err(_e) => {
                            Err(CollectError::CollectError("failed to collect tx".to_string()))
                        }
                    };
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

pub(crate) async fn fetch_block_state_diffs(
    chunk: &BlockChunk,
    source: &Source<Provider<impl JsonRpcClient>>,
) -> mpsc::Receiver<BlockNumberTransactionsTraces> {
    fetch_block_traces(chunk, &[TraceType::StateDiff], source).await
}

pub(crate) async fn fetch_transaction_state_diffs(
    chunk: &TransactionChunk,
    source: &Source<Provider<impl JsonRpcClient>>,
    include_indices: bool,
) -> mpsc::Receiver<BlockNumberTransactionsTraces> {
    fetch_transaction_traces(chunk, &[TraceType::StateDiff], source, include_indices).await
}

async fn state_diffs_to_df(
    mut rx: mpsc::Receiver<BlockNumberTransactionsTraces>,
    schemas: &HashMap<Datatype, Table>,
    chain_id: u64,
) -> Result<HashMap<Datatype, DataFrame>, CollectError> {
    let include_storage = schemas.contains_key(&Datatype::StorageDiffs);
    let include_balance = schemas.contains_key(&Datatype::BalanceDiffs);
    let include_nonce = schemas.contains_key(&Datatype::NonceDiffs);
    let include_code = schemas.contains_key(&Datatype::CodeDiffs);

    let capacity = 0;

    // storage
    let include_storage_block_number = included(schemas, Datatype::StorageDiffs, "block_number");
    let include_storage_transaction_index =
        included(schemas, Datatype::StorageDiffs, "transaction_index");
    let include_storage_transaction_hash =
        included(schemas, Datatype::StorageDiffs, "transaction_hash");
    let include_storage_address = included(schemas, Datatype::StorageDiffs, "address");
    let include_storage_slot = included(schemas, Datatype::StorageDiffs, "slot");
    let include_storage_from_value = included(schemas, Datatype::StorageDiffs, "from_value");
    let include_storage_to_value = included(schemas, Datatype::StorageDiffs, "to_value");
    let mut storage_block_number: Vec<u32> = Vec::with_capacity(capacity);
    let mut storage_transaction_index: Vec<u32> = Vec::with_capacity(capacity);
    let mut storage_transaction_hash: Vec<Vec<u8>> = Vec::with_capacity(capacity);
    let mut storage_address: Vec<Vec<u8>> = Vec::with_capacity(capacity);
    let mut storage_slot: Vec<Vec<u8>> = Vec::with_capacity(capacity);
    let mut storage_from_value: Vec<Vec<u8>> = Vec::with_capacity(capacity);
    let mut storage_to_value: Vec<Vec<u8>> = Vec::with_capacity(capacity);

    // balance
    let include_balance_block_number = included(schemas, Datatype::BalanceDiffs, "block_number");
    let include_balance_transaction_index =
        included(schemas, Datatype::BalanceDiffs, "transaction_index");
    let include_balance_transaction_hash =
        included(schemas, Datatype::BalanceDiffs, "transaction_hash");
    let include_balance_address = included(schemas, Datatype::BalanceDiffs, "address");
    let include_balance_from_value = included(schemas, Datatype::BalanceDiffs, "from_value");
    let include_balance_to_value = included(schemas, Datatype::BalanceDiffs, "to_value");
    let mut balance_block_number: Vec<u32> = Vec::with_capacity(capacity);
    let mut balance_transaction_index: Vec<u32> = Vec::with_capacity(capacity);
    let mut balance_transaction_hash: Vec<Vec<u8>> = Vec::with_capacity(capacity);
    let mut balance_address: Vec<Vec<u8>> = Vec::with_capacity(capacity);
    let mut balance_from_value: Vec<String> = Vec::with_capacity(capacity);
    let mut balance_to_value: Vec<String> = Vec::with_capacity(capacity);

    // nonce
    let include_nonce_block_number = included(schemas, Datatype::NonceDiffs, "block_number");
    let include_nonce_transaction_index =
        included(schemas, Datatype::NonceDiffs, "transaction_index");
    let include_nonce_transaction_hash =
        included(schemas, Datatype::NonceDiffs, "transaction_hash");
    let include_nonce_address = included(schemas, Datatype::NonceDiffs, "address");
    let include_nonce_from_value = included(schemas, Datatype::NonceDiffs, "from_value");
    let include_nonce_to_value = included(schemas, Datatype::NonceDiffs, "to_value");
    let mut nonce_block_number: Vec<u32> = Vec::with_capacity(capacity);
    let mut nonce_transaction_index: Vec<u32> = Vec::with_capacity(capacity);
    let mut nonce_transaction_hash: Vec<Vec<u8>> = Vec::with_capacity(capacity);
    let mut nonce_address: Vec<Vec<u8>> = Vec::with_capacity(capacity);
    let mut nonce_from_value: Vec<u64> = Vec::with_capacity(capacity);
    let mut nonce_to_value: Vec<u64> = Vec::with_capacity(capacity);

    // code
    let include_code_block_number = included(schemas, Datatype::CodeDiffs, "block_number");
    let include_code_transaction_index =
        included(schemas, Datatype::CodeDiffs, "transaction_index");
    let include_code_transaction_hash = included(schemas, Datatype::CodeDiffs, "transaction_hash");
    let include_code_address = included(schemas, Datatype::CodeDiffs, "address");
    let include_code_from_value = included(schemas, Datatype::CodeDiffs, "from_value");
    let include_code_to_value = included(schemas, Datatype::CodeDiffs, "to_value");
    let mut code_block_number: Vec<u32> = Vec::with_capacity(capacity);
    let mut code_transaction_index: Vec<u32> = Vec::with_capacity(capacity);
    let mut code_transaction_hash: Vec<Vec<u8>> = Vec::with_capacity(capacity);
    let mut code_address: Vec<Vec<u8>> = Vec::with_capacity(capacity);
    let mut code_from_value: Vec<Vec<u8>> = Vec::with_capacity(capacity);
    let mut code_to_value: Vec<Vec<u8>> = Vec::with_capacity(capacity);

    let mut n_rows = 0;
    while let Some(message) = rx.recv().await {
        match message {
            Ok((block_num, blocks_traces)) => {
                for (t_index, ts) in blocks_traces.iter() {
                    if let (Some(tx), Some(StateDiff(state_diff))) =
                        (ts.transaction_hash, &ts.state_diff)
                    {
                        for (addr, addr_diff) in state_diff.iter() {
                            n_rows += n_rows;

                            // storage
                            if include_storage {
                                for (s, diff) in addr_diff.storage.iter() {
                                    let (from, to) = match diff {
                                        Diff::Same => (H256::zero(), H256::zero()),
                                        Diff::Born(value) => (H256::zero(), *value),
                                        Diff::Died(value) => (*value, H256::zero()),
                                        Diff::Changed(ChangedType { from, to }) => (*from, *to),
                                    };
                                    if include_storage_block_number {
                                        match block_num {
                                            Some(block_num) => storage_block_number.push(block_num),
                                            None => {
                                                return Err(CollectError::CollectError(
                                                    "block number not given".to_string(),
                                                ))
                                            }
                                        }
                                    };
                                    if include_storage_transaction_index {
                                        storage_transaction_index.push(*t_index);
                                    };
                                    if include_storage_transaction_hash {
                                        storage_transaction_hash.push(tx.as_bytes().to_vec());
                                    };
                                    if include_storage_address {
                                        storage_address.push(addr.as_bytes().to_vec());
                                    };
                                    if include_storage_slot {
                                        storage_slot.push(s.as_bytes().to_vec());
                                    };
                                    if include_storage_from_value {
                                        storage_from_value.push(from.as_bytes().to_vec());
                                    };
                                    if include_storage_to_value {
                                        storage_to_value.push(to.as_bytes().to_vec());
                                    };
                                }
                            }

                            // balance
                            if include_balance {
                                let (from, to) = match addr_diff.balance {
                                    Diff::Same => ("0".to_string(), "0".to_string()),
                                    Diff::Born(value) => ("0".to_string(), value.to_string()),
                                    Diff::Died(value) => (value.to_string(), "0".to_string()),
                                    Diff::Changed(ChangedType { from, to }) => {
                                        (from.to_string(), to.to_string())
                                    }
                                };
                                if include_balance_block_number {
                                    match block_num {
                                        Some(block_num) => balance_block_number.push(block_num),
                                        None => {
                                            return Err(CollectError::CollectError(
                                                "block number not given".to_string(),
                                            ))
                                        }
                                    }
                                };
                                if include_balance_transaction_index {
                                    balance_transaction_index.push(*t_index);
                                };
                                if include_balance_transaction_hash {
                                    balance_transaction_hash.push(tx.as_bytes().to_vec());
                                };
                                if include_balance_address {
                                    balance_address.push(addr.as_bytes().to_vec());
                                };
                                if include_balance_from_value {
                                    balance_from_value.push(from);
                                };
                                if include_balance_to_value {
                                    balance_to_value.push(to);
                                };
                            }

                            // nonce
                            if include_nonce {
                                let (from, to) = match addr_diff.nonce {
                                    Diff::Same => (0u64, 0u64),
                                    Diff::Born(value) => (0u64, value.as_u64()),
                                    Diff::Died(value) => (value.as_u64(), 0u64),
                                    Diff::Changed(ChangedType { from, to }) => {
                                        (from.as_u64(), to.as_u64())
                                    }
                                };
                                if include_nonce_block_number {
                                    match block_num {
                                        Some(block_num) => nonce_block_number.push(block_num),
                                        None => {
                                            return Err(CollectError::CollectError(
                                                "block number not given".to_string(),
                                            ))
                                        }
                                    }
                                };
                                if include_nonce_transaction_index {
                                    nonce_transaction_index.push(*t_index);
                                };
                                if include_nonce_transaction_hash {
                                    nonce_transaction_hash.push(tx.as_bytes().to_vec());
                                };
                                if include_nonce_address {
                                    nonce_address.push(addr.as_bytes().to_vec());
                                };
                                if include_nonce_from_value {
                                    nonce_from_value.push(from);
                                };
                                if include_nonce_to_value {
                                    nonce_to_value.push(to);
                                };
                            }

                            // code
                            if include_code {
                                let (from, to) = match &addr_diff.code {
                                    Diff::Same => (
                                        H256::zero().as_bytes().to_vec(),
                                        H256::zero().as_bytes().to_vec(),
                                    ),
                                    Diff::Born(value) => {
                                        (H256::zero().as_bytes().to_vec(), value.to_vec())
                                    }
                                    Diff::Died(value) => {
                                        (value.to_vec(), H256::zero().as_bytes().to_vec())
                                    }
                                    Diff::Changed(ChangedType { from, to }) => {
                                        (from.to_vec(), to.to_vec())
                                    }
                                };
                                if include_code_block_number {
                                    match block_num {
                                        Some(block_num) => code_block_number.push(block_num),
                                        None => {
                                            return Err(CollectError::CollectError(
                                                "block number not given".to_string(),
                                            ))
                                        }
                                    }
                                };
                                if include_code_transaction_index {
                                    code_transaction_index.push(*t_index);
                                };
                                if include_code_transaction_hash {
                                    code_transaction_hash.push(tx.as_bytes().to_vec());
                                };
                                if include_code_address {
                                    code_address.push(addr.as_bytes().to_vec());
                                };
                                if include_code_from_value {
                                    code_from_value.push(from);
                                };
                                if include_code_to_value {
                                    code_to_value.push(to);
                                };
                            }
                        }
                    }
                }
            }
            _ => return Err(CollectError::TooManyRequestsError),
        }
    }

    let mut dfs: HashMap<Datatype, DataFrame> = HashMap::new();

    // storage
    if include_storage {
        let mut cols = Vec::new();
        let schema = &schemas[&Datatype::StorageDiffs];
        with_series!(cols, "block_number", storage_block_number, schema);
        with_series!(cols, "transaction_index", storage_transaction_index, schema);
        with_series_binary!(cols, "transaction_hash", storage_transaction_hash, schema);
        with_series_binary!(cols, "address", storage_address, schema);
        with_series_binary!(cols, "slot", storage_slot, schema);
        with_series_binary!(cols, "from_value", storage_from_value, schema);
        with_series_binary!(cols, "to_value", storage_to_value, schema);
        if schema.has_column("chain_id") {
            cols.push(Series::new("chain_id", vec![chain_id; n_rows]));
        }
        let df = DataFrame::new(cols)?;
        dfs.insert(Datatype::StorageDiffs, df);
    };

    // balance
    if include_balance {
        let mut cols = Vec::new();
        let schema = &schemas[&Datatype::BalanceDiffs];
        with_series!(cols, "block_number", balance_block_number, schema);
        with_series!(cols, "transaction_index", balance_transaction_index, schema);
        with_series_binary!(cols, "transaction_hash", balance_transaction_hash, schema);
        with_series_binary!(cols, "address", balance_address, schema);
        with_series!(cols, "from_value", balance_from_value, schema);
        with_series!(cols, "to_value", balance_to_value, schema);
        if schema.has_column("chain_id") {
            cols.push(Series::new("chain_id", vec![chain_id; n_rows]));
        }
        let df = DataFrame::new(cols)?;
        dfs.insert(Datatype::BalanceDiffs, df);
    };

    // nonce
    if include_nonce {
        let mut cols = Vec::new();
        let schema = &schemas[&Datatype::NonceDiffs];
        with_series!(cols, "block_number", nonce_block_number, schema);
        with_series!(cols, "transaction_index", nonce_transaction_index, schema);
        with_series_binary!(cols, "transaction_hash", nonce_transaction_hash, schema);
        with_series_binary!(cols, "address", nonce_address, schema);
        with_series!(cols, "from_value", nonce_from_value, schema);
        with_series!(cols, "to_value", nonce_to_value, schema);
        if schema.has_column("chain_id") {
            cols.push(Series::new("chain_id", vec![chain_id; n_rows]));
        }
        let df = DataFrame::new(cols)?;
        dfs.insert(Datatype::NonceDiffs, df);
    };

    // code
    if include_code {
        let mut cols = Vec::new();
        let schema = &schemas[&Datatype::CodeDiffs];
        with_series!(cols, "block_number", code_block_number, schema);
        with_series!(cols, "transaction_index", code_transaction_index, schema);
        with_series_binary!(cols, "transaction_hash", code_transaction_hash, schema);
        with_series_binary!(cols, "address", code_address, schema);
        with_series_binary!(cols, "from_value", code_from_value, schema);
        with_series_binary!(cols, "to_value", code_to_value, schema);
        if schema.has_column("chain_id") {
            cols.push(Series::new("chain_id", vec![chain_id; n_rows]));
        }
        let df = DataFrame::new(cols)?;
        dfs.insert(Datatype::CodeDiffs, df);
    };

    Ok(dfs)
}

fn included(
    schemas: &HashMap<Datatype, Table>,
    datatype: Datatype,
    column_name: &'static str,
) -> bool {
    if let Some(schema) = schemas.get(&datatype) {
        schema.has_column(column_name)
    } else {
        false
    }
}
