use std::collections::{HashMap, HashSet};

use ethers::prelude::*;
use polars::prelude::*;
use tokio::{sync::mpsc, task};

use super::blocks;
use crate::{
    sources::Fetcher,
    types::{
        BlockChunk, BlocksAndTransactions, CollectError, Datatype, MultiDataset, RowFilter, Source,
        Table,
    },
};

#[async_trait::async_trait]
impl MultiDataset for BlocksAndTransactions {
    fn name(&self) -> &'static str {
        "blocks_and_transactions"
    }

    fn datatypes(&self) -> HashSet<Datatype> {
        [Datatype::Blocks, Datatype::Transactions].into_iter().collect()
    }

    async fn collect_block_chunk(
        &self,
        chunk: &BlockChunk,
        source: &Source,
        schemas: HashMap<Datatype, Table>,
        _filter: HashMap<Datatype, RowFilter>,
    ) -> Result<HashMap<Datatype, DataFrame>, CollectError> {
        let include_gas_used = match &schemas.get(&Datatype::Transactions) {
            Some(table) => table.has_column("gas_used"),
            _ => false,
        };
        let rx = fetch_blocks_and_transactions(chunk, source, include_gas_used).await;
        let output = blocks::blocks_to_dfs(
            rx,
            &schemas.get(&Datatype::Blocks),
            &schemas.get(&Datatype::Transactions),
            source.chain_id,
        )
        .await;
        match output {
            Ok((Some(blocks_df), Some(txs_df))) => {
                let mut output: HashMap<Datatype, DataFrame> = HashMap::new();
                output.insert(Datatype::Blocks, blocks_df);
                output.insert(Datatype::Transactions, txs_df);
                Ok(output)
            }
            Ok((_, _)) => Err(CollectError::BadSchemaError),
            Err(e) => Err(e),
        }
    }
}

pub(crate) async fn fetch_blocks_and_transactions(
    block_chunk: &BlockChunk,
    source: &Source,
    include_gas_used: bool,
) -> mpsc::Receiver<blocks::BlockTxGasTuple<Transaction>> {
    let (tx, rx) = mpsc::channel(block_chunk.numbers().len());
    let source = Arc::new(source.clone());

    for number in block_chunk.numbers() {
        let tx = tx.clone();
        let fetcher = source.fetcher.clone();
        task::spawn(async move {
            let block_result = fetcher.get_block_with_txs(number).await;

            // get gas usage
            let result = match block_result {
                Ok(Some(block)) => {
                    if include_gas_used {
                        match get_txs_gas_used(&block, fetcher).await {
                            Ok(gas_used) => Ok((block, Some(gas_used))),
                            Err(e) => Err(e),
                        }
                    } else {
                        Ok((block, None))
                    }
                }
                Ok(None) => Err(CollectError::CollectError("no block found".into())),
                Err(e) => Err(e),
            };

            // send to channel
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

async fn get_txs_gas_used<P: JsonRpcClient + 'static>(
    block: &Block<Transaction>,
    fetcher: Arc<Fetcher<P>>,
) -> Result<Vec<u32>, CollectError> {
    match get_txs_gas_used_per_block(block, fetcher.clone()).await {
        Ok(value) => Ok(value),
        Err(_) => get_txs_gas_used_per_tx(block, fetcher).await,
    }
}

async fn get_txs_gas_used_per_block<P: JsonRpcClient + 'static>(
    block: &Block<Transaction>,
    fetcher: Arc<Fetcher<P>>,
) -> Result<Vec<u32>, CollectError> {
    let block_number = match block.number {
        Some(number) => number,
        None => return Err(CollectError::CollectError("no block number".to_string())),
    };
    let receipts = fetcher.get_block_receipts(block_number.as_u64()).await?;
    let mut gas_used: Vec<u32> = Vec::new();
    for receipt in receipts {
        match receipt.gas_used {
            Some(value) => gas_used.push(value.as_u32()),
            None => return Err(CollectError::CollectError("no gas_used for tx".to_string())),
        }
    }
    Ok(gas_used)
}

async fn get_txs_gas_used_per_tx<P: JsonRpcClient + 'static>(
    block: &Block<Transaction>,
    fetcher: Arc<Fetcher<P>>,
) -> Result<Vec<u32>, CollectError> {
    let mut tasks = Vec::new();
    for tx in &block.transactions {
        let tx_clone = tx.hash;
        let fetcher = fetcher.clone();
        let task = task::spawn(async move {
            match fetcher.get_transaction_receipt(tx_clone).await? {
                Some(receipt) => Ok(receipt.gas_used),
                None => Err(CollectError::CollectError("could not find tx receipt".to_string())),
            }
        });
        tasks.push(task);
    }

    let mut gas_used: Vec<u32> = Vec::new();
    for task in tasks {
        match task.await {
            Ok(Ok(Some(value))) => gas_used.push(value.as_u32()),
            _ => return Err(CollectError::CollectError("gas_used not available from node".into())),
        }
    }

    Ok(gas_used)
}
