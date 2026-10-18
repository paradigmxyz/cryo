use crate::*;
use alloy::{
    primitives::{Address, TxHash},
    rpc::types::{
        eth::{Block, Log},
        trace::parity::{Action, LocalizedTransactionTrace, TraceOutput},
        BlockTransactionsKind, Filter, FilterBlockOption,
    },
    sol_types::SolEvent,
};
use polars::prelude::*;
use std::collections::HashMap;

/// columns for transactions
#[derive(Default, cryo_to_df::ToDataFrames)]
pub struct AddressAppearances {
    n_rows: usize,
    block_number: Vec<u32>,
    block_hash: Vec<RawBytes>,
    transaction_hash: Vec<RawBytes>,
    address: Vec<RawBytes>,
    relationship: Vec<String>,
    chain_id: Vec<u64>,
}

#[async_trait::async_trait]
impl Dataset for AddressAppearances {
    fn default_columns() -> Option<Vec<&'static str>> {
        Some(vec![
            "block_number",
            // "block_hash",
            "transaction_hash",
            "address",
            "relationship",
            "chain_id",
        ])
    }

    fn default_sort() -> Option<Vec<&'static str>> {
        Some(vec!["block_number", "transaction_hash", "address", "relationship"])
    }
}

type BlockLogsTraces = (Block, Vec<Log>, Vec<LocalizedTransactionTrace>);

#[async_trait::async_trait]
impl CollectByBlock for AddressAppearances {
    type Response = BlockLogsTraces;

    async fn extract(request: Params, source: Arc<Source>, _: Arc<Query>) -> R<Self::Response> {
        let block_number = request.ethers_block_number()?;
        let block =
            source.get_block(request.block_number()?, BlockTransactionsKind::Hashes).await?;
        let block = block.ok_or(CollectError::CollectError("block not found".to_string()))?;
        let filter = Filter {
            block_option: FilterBlockOption::Range {
                from_block: Some(block_number.into()),
                to_block: Some(block_number.into()),
            },
            ..Default::default()
        };
        let logs = source.get_logs(&filter).await?;
        let traces = source.trace_block(request.block_number()?).await?;
        Ok((block, logs, traces))
    }

    fn transform(response: Self::Response, columns: &mut Self, query: &Arc<Query>) -> R<()> {
        let schema = query.schemas.get_schema(&Datatype::AddressAppearances)?;
        process_appearances(response, columns, schema)
    }
}

#[async_trait::async_trait]
impl CollectByTransaction for AddressAppearances {
    type Response = BlockLogsTraces;

    async fn extract(request: Params, source: Arc<Source>, _: Arc<Query>) -> R<Self::Response> {
        let tx_hash = request.ethers_transaction_hash()?;

        let tx_data = source.get_transaction_by_hash(tx_hash).await?.ok_or_else(|| {
            CollectError::CollectError("could not find transaction data".to_string())
        })?;

        let block_number = tx_data
            .block_number
            .ok_or_else(|| CollectError::CollectError("block not found".to_string()))?;
        let block = source
            .get_block(block_number, BlockTransactionsKind::Hashes)
            .await?
            .ok_or(CollectError::CollectError("could not get block".to_string()))?;

        // logs
        let logs = source
            .get_transaction_receipt(tx_hash)
            .await?
            .ok_or(CollectError::CollectError("could not get tx receipt".to_string()))?
            .inner
            .logs()
            .to_vec();

        // traces
        let traces = source.trace_transaction(request.ethers_transaction_hash()?).await?;

        Ok((block, logs, traces))
    }

    fn transform(response: Self::Response, columns: &mut Self, query: &Arc<Query>) -> R<()> {
        let schema = query.schemas.get_schema(&Datatype::AddressAppearances)?;
        process_appearances(response, columns, schema)
    }
}

fn name(log: &Log) -> Option<&'static str> {
    let event = log.topic0().unwrap();
    if event == *ERC20::Transfer::SIGNATURE_HASH {
        if !log.data().data.is_empty() {
            Some("erc20_transfer")
        } else if log.topics().len() == 4 {
            Some("erc721_transfer")
        } else {
            None
        }
    } else {
        None
    }
}

impl AddressAppearances {
    fn process_first_transaction(
        &mut self,
        block_author: Address,
        trace: &LocalizedTransactionTrace,
        schema: &Table,
        tx_hash: TxHash,
        logs_by_tx: &HashMap<TxHash, Vec<Log>>,
    ) {
        let block_number = trace.block_number.unwrap() as u32;
        let block_hash = trace.block_hash.unwrap().to_vec();
        self.process_address(block_author, "miner_fee", block_number, &block_hash, tx_hash, schema);

        if let Some(logs) = logs_by_tx.get(&tx_hash) {
            for log in logs.iter() {
                if log.topics().len() >= 3 {
                    if let Some(name) = name(log) {
                        let mut from: [u8; 20] = [0; 20];
                        from.copy_from_slice(&log.topics()[1][12..32]);

                        let name = &(name.to_string() + "_from");
                        self.process_address(
                            Address::from_slice(&from),
                            name,
                            block_number,
                            &block_hash,
                            tx_hash,
                            schema,
                        );

                        let mut to: [u8; 20] = [0; 20];
                        to.copy_from_slice(&log.topics()[1][12..32]);
                        let name = &(name.to_string() + "_to");
                        self.process_address(
                            Address::from_slice(&to),
                            name,
                            block_number,
                            &block_hash,
                            tx_hash,
                            schema,
                        );
                    }
                }
            }
        }

        match &trace.trace.action {
            Action::Call(action) => {
                self.process_address(
                    action.from,
                    "tx_from",
                    block_number,
                    &block_hash,
                    tx_hash,
                    schema,
                );
                self.process_address(
                    action.to,
                    "tx_to",
                    block_number,
                    &block_hash,
                    tx_hash,
                    schema,
                );
            }
            Action::Create(action) => {
                self.process_address(
                    action.from,
                    "tx_from",
                    block_number,
                    &block_hash,
                    tx_hash,
                    schema,
                );
            }
            _ => {}
        }

        if let Some(TraceOutput::Create(result)) = &trace.trace.result {
            self.process_address(
                result.address,
                "tx_to",
                block_number,
                &block_hash,
                tx_hash,
                schema,
            );
        }
    }

    fn process_trace(
        &mut self,
        trace: &LocalizedTransactionTrace,
        schema: &Table,
        tx_hash: TxHash,
    ) {
        let block_number = trace.block_number.unwrap() as u32;
        let block_hash = trace.block_hash.unwrap().to_vec();
        match &trace.trace.action {
            Action::Call(action) => {
                self.process_address(
                    action.from,
                    "call_from",
                    block_number,
                    &block_hash,
                    tx_hash,
                    schema,
                );
                self.process_address(
                    action.to,
                    "call_to",
                    block_number,
                    &block_hash,
                    tx_hash,
                    schema,
                );
            }
            Action::Create(action) => {
                self.process_address(
                    action.from,
                    "factory",
                    block_number,
                    &block_hash,
                    tx_hash,
                    schema,
                );
            }
            Action::Selfdestruct(action) => {
                self.process_address(
                    action.address,
                    "suicide",
                    block_number,
                    &block_hash,
                    tx_hash,
                    schema,
                );
                self.process_address(
                    action.refund_address,
                    "suicide_refund",
                    block_number,
                    &block_hash,
                    tx_hash,
                    schema,
                );
            }
            Action::Reward(action) => {
                self.process_address(
                    action.author,
                    "author",
                    block_number,
                    &block_hash,
                    tx_hash,
                    schema,
                );
            }
        }

        if let Some(TraceOutput::Create(result)) = &trace.trace.result {
            self.process_address(
                result.address,
                "create",
                block_number,
                &block_hash,
                tx_hash,
                schema,
            );
        };
    }

    fn process_address(
        &mut self,
        address: Address,
        relationship: &str,
        block_number: u32,
        block_hash: &[u8],
        transaction_hash: TxHash,
        schema: &Table,
    ) {
        self.n_rows += 1;
        store!(schema, self, address, address.to_vec());
        store!(schema, self, relationship, relationship.to_string());
        store!(schema, self, block_number, block_number);
        store!(schema, self, block_hash, block_hash.to_vec());
        store!(schema, self, transaction_hash, transaction_hash.to_vec());
    }
}

fn process_appearances(
    traces: BlockLogsTraces,
    columns: &mut AddressAppearances,
    schema: &Table,
) -> R<()> {
    let (block, logs, traces) = traces;
    let mut logs_by_tx: HashMap<TxHash, Vec<Log>> = HashMap::new();
    for log in logs.into_iter() {
        if let Some(tx_hash) = log.transaction_hash {
            logs_by_tx.entry(tx_hash).or_default().push(log);
        }
    }

    let (_block_number, block_author) = (block.header.number, block.header.beneficiary);

    let mut current_tx_hash = TxHash::ZERO;
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

    Ok(())
}
