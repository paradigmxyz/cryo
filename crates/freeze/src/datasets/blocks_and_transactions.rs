use std::collections::HashMap;
use std::collections::HashSet;

use ethers::prelude::*;
use polars::prelude::*;
use tokio::sync::mpsc;
use tokio::task;

use super::blocks;
use crate::types::BlockChunk;
use crate::types::BlocksAndTransactions;
use crate::types::CollectError;
use crate::types::Datatype;
use crate::types::FetchOpts;
use crate::types::FreezeOpts;
use crate::types::MultiDataset;

#[async_trait::async_trait]
impl MultiDataset for BlocksAndTransactions {
    fn name(&self) -> &'static str {
        "blocks_and_transactions"
    }

    fn datatypes(&self) -> HashSet<Datatype> {
        [Datatype::Blocks, Datatype::Transactions]
            .into_iter()
            .collect()
    }

    async fn collect_block_chunk(
        &self,
        chunk: &BlockChunk,
        opts: &FreezeOpts,
    ) -> Result<HashMap<Datatype, DataFrame>, CollectError> {
        let rx = fetch_blocks_and_transactions(chunk, &opts.chunk_fetch_opts()).await;
        let output = blocks::blocks_to_dfs(
            rx,
            &opts.schemas.get(&Datatype::Blocks),
            &opts.schemas.get(&Datatype::Transactions),
            opts.chain_id,
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
    opts: &FetchOpts,
) -> mpsc::Receiver<Result<Option<Block<Transaction>>, CollectError>> {
    let (tx, rx) = mpsc::channel(block_chunk.numbers().len());

    for number in block_chunk.numbers() {
        let tx = tx.clone();
        let provider = opts.provider.clone();
        let semaphore = opts.semaphore.clone();
        let rate_limiter = opts.rate_limiter.as_ref().map(Arc::clone);
        task::spawn(async move {
            let _permit = Arc::clone(&semaphore).acquire_owned().await;
            if let Some(limiter) = rate_limiter {
                Arc::clone(&limiter).until_ready().await;
            }
            let block = provider
                .get_block_with_txs(number)
                .await
                .map_err(CollectError::ProviderError);
            match tx.send(block).await {
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
