use crate::{
    conversions::ToVecHex, dataframes::SortableDataFrame, store, with_series, with_series_binary,
    CollectByBlock, CollectByTransaction, CollectError, ColumnData, ColumnType, Datatype, Params,
    Schemas, Source, Table, TransactionAddresses,
};
use ethers::prelude::*;
use polars::prelude::*;
use std::collections::HashMap;

type Result<T> = ::core::result::Result<T, CollectError>;

type BlockLogsTraces = (Block<TxHash>, Vec<Log>, Vec<Trace>);

lazy_static::lazy_static! {
    /// event hash of ERC20_TRANSFER
    pub static ref ERC20_TRANSFER: H256 = H256(
        prefix_hex::decode("0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef")
            .expect("Decoding failed"),
    );
}

#[async_trait::async_trait]
impl CollectByBlock for TransactionAddresses {
    type Response = BlockLogsTraces;

    type Columns = TransactionAddressColumns;

    async fn extract(request: Params, source: Source, _schemas: Schemas) -> Result<Self::Response> {
        let block_number = request.ethers_block_number();
        let block = source.fetcher.get_block(request.block_number()).await?;
        let block = block.ok_or(CollectError::CollectError("block not found".to_string()))?;
        let filter = Filter {
            block_option: FilterBlockOption::Range {
                from_block: Some(block_number),
                to_block: Some(block_number),
            },
            ..Default::default()
        };
        let logs = source.fetcher.get_logs(&filter).await?;
        let traces = source.fetcher.trace_block(request.block_number().into()).await?;
        Ok((block, logs, traces))
    }

    fn transform(response: Self::Response, columns: &mut Self::Columns, schemas: &Schemas) {
        let schema = schemas.get(&Datatype::TransactionAddresses).expect("schema not provided");
        process_appearances(response, columns, schema)
    }
}

#[async_trait::async_trait]
impl CollectByTransaction for TransactionAddresses {
    type Response = BlockLogsTraces;

    type Columns = TransactionAddressColumns;

    async fn extract(request: Params, source: Source, _schemas: Schemas) -> Result<Self::Response> {
        let tx_hash = request.ethers_transaction_hash();

        let tx_data = source.fetcher.get_transaction(tx_hash).await?.ok_or_else(|| {
            CollectError::CollectError("could not find transaction data".to_string())
        })?;

        let block_number = tx_data
            .block_number
            .ok_or_else(|| CollectError::CollectError("block not found".to_string()))?
            .as_u64();
        let block = source
            .fetcher
            .get_block(block_number)
            .await?
            .ok_or(CollectError::CollectError("could not get block".to_string()))?;

        // logs
        let logs = source
            .fetcher
            .get_transaction_receipt(tx_hash)
            .await?
            .ok_or(CollectError::CollectError("could not get tx receipt".to_string()))?
            .logs;

        // traces
        let traces = source.fetcher.trace_transaction(request.ethers_transaction_hash()).await?;

        Ok((block, logs, traces))
    }

    fn transform(response: Self::Response, columns: &mut Self::Columns, schemas: &Schemas) {
        let schema = schemas.get(&Datatype::TransactionAddresses).expect("schema not provided");
        process_appearances(response, columns, schema)
    }
}

fn name(log: &Log) -> Option<&'static str> {
    let event = log.topics[0];
    if event == *ERC20_TRANSFER {
        if log.data.len() > 0 {
            Some("erc20_transfer")
        } else if log.topics.len() == 4 {
            Some("erc721_transfer")
        } else {
            None
        }
    } else {
        None
    }
}

/// columns for transactions
#[cryo_to_df::to_df(Datatype::TransactionAddresses)]
#[derive(Default)]
pub struct TransactionAddressColumns {
    n_rows: usize,
    block_number: Vec<u32>,
    transaction_hash: Vec<Vec<u8>>,
    address: Vec<Vec<u8>>,
    relationship: Vec<String>,
}

impl TransactionAddressColumns {
    fn process_first_transaction(
        &mut self,
        block_author: H160,
        trace: &Trace,
        schema: &Table,
        tx_hash: H256,
        logs_by_tx: &HashMap<H256, Vec<Log>>,
    ) {
        let block_number = trace.block_number as u32;
        self.process_address(block_author, "miner_fee", block_number, tx_hash, schema);

        if let Some(logs) = logs_by_tx.get(&tx_hash) {
            for log in logs.iter() {
                if log.topics.len() >= 3 {
                    if let Some(name) = name(log) {
                        let mut from: [u8; 20] = [0; 20];
                        from.copy_from_slice(&log.topics[1].to_fixed_bytes()[12..32]);

                        let name = &(name.to_string() + "_from");
                        self.process_address(H160(from), name, block_number, tx_hash, schema);

                        let mut to: [u8; 20] = [0; 20];
                        to.copy_from_slice(&log.topics[1].to_fixed_bytes()[12..32]);
                        let name = &(name.to_string() + "_to");
                        self.process_address(H160(to), name, block_number, tx_hash, schema);
                    }
                }
            }
        }

        match &trace.action {
            Action::Call(action) => {
                self.process_address(action.from, "tx_from", block_number, tx_hash, schema);
                self.process_address(action.to, "tx_to", block_number, tx_hash, schema);
            }
            Action::Create(action) => {
                self.process_address(action.from, "tx_from", block_number, tx_hash, schema);
            }
            _ => panic!("invalid first tx trace"),
        }

        if let Some(Res::Create(result)) = &trace.result {
            self.process_address(result.address, "tx_to", block_number, tx_hash, schema);
        }
    }

    fn process_trace(
        &mut self,
        trace: &Trace,
        schema: &Table,
        tx_hash: H256,
    ) {
        let block_number = trace.block_number as u32;
        match &trace.action {
            Action::Call(action) => {
                self.process_address(action.from, "call_from", block_number, tx_hash, schema);
                self.process_address(action.to, "call_to", block_number, tx_hash, schema);
            }
            Action::Create(action) => {
                self.process_address(action.from, "factory", block_number, tx_hash, schema);
            }
            Action::Suicide(action) => {
                self.process_address(action.address, "suicide", block_number, tx_hash, schema);
                self.process_address(
                    action.refund_address,
                    "suicide_refund",
                    block_number,
                    tx_hash,
                    schema,
                );
            }
            Action::Reward(action) => {
                self.process_address(action.author, "author", block_number, tx_hash, schema);
            }
        }

        if let Some(Res::Create(result)) = &trace.result {
            self.process_address(result.address, "create", block_number, tx_hash, schema);
        };
    }

    fn process_address(
        &mut self,
        address: H160,
        relationship: &str,
        block_number: u32,
        transaction_hash: H256,
        schema: &Table,
    ) {
        self.n_rows += 1;
        store!(schema, self, address, address.as_bytes().to_vec());
        store!(schema, self, relationship, relationship.to_string());
        store!(schema, self, block_number, block_number);
        store!(schema, self, transaction_hash, transaction_hash.as_bytes().to_vec());
    }
}

fn process_appearances(
    traces: BlockLogsTraces,
    columns: &mut TransactionAddressColumns,
    schema: &Table,
) {
    let (block, logs, traces) = traces;
    let mut logs_by_tx: HashMap<H256, Vec<Log>> = HashMap::new();
    for log in logs.into_iter() {
        if let Some(tx_hash) = log.transaction_hash {
            logs_by_tx.entry(tx_hash).or_default().push(log);
        }
    }

    let (_block_number, block_author) = match (block.number, block.author) {
        (Some(number), Some(author)) => (number.as_u64(), author),
        _ => return,
    };

    let mut current_tx_hash = H256([0; 32]);
    for trace in traces.iter() {
        if let (Some(tx_hash), Some(_tx_pos)) = (trace.transaction_hash, trace.transaction_position)
        {
            if tx_hash != current_tx_hash {
                columns.process_first_transaction(block_author, trace, schema, tx_hash, &logs_by_tx)
            }
            columns.process_trace(trace, schema, tx_hash);
            current_tx_hash = tx_hash;
        }
    }
}
