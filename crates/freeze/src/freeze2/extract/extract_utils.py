# use crate::{CollectError, Fetcher};
# use ethers::prelude::*;
# use polars::prelude::*;
# use tokio::task;

# /// get gas used by transactions in block
# pub async fn get_txs_gas_used<P: JsonRpcClient + 'static>(
#     block: &Block<Transaction>,
#     fetcher: Arc<Fetcher<P>>,
# ) -> Result<Vec<u32>, CollectError> {
#     match get_txs_gas_used_per_block(block, fetcher.clone()).await {
#         Ok(value) => Ok(value),
#         Err(_) => get_txs_gas_used_per_tx(block, fetcher).await,
#     }
# }

# async fn get_txs_gas_used_per_block<P: JsonRpcClient + 'static>(
#     block: &Block<Transaction>,
#     fetcher: Arc<Fetcher<P>>,
# ) -> Result<Vec<u32>, CollectError> {
#     let block_number = match block.number {
#         Some(number) => number,
#         None => return Err(CollectError::CollectError("no block number".to_string())),
#     };
#     let receipts = fetcher.get_block_receipts(block_number.as_u64()).await?;
#     let mut gas_used: Vec<u32> = Vec::new();
#     for receipt in receipts {
#         match receipt.gas_used {
#             Some(value) => gas_used.push(value.as_u32()),
#             None => return Err(CollectError::CollectError("no gas_used for tx".to_string())),
#         }
#     }
#     Ok(gas_used)
# }

# async fn get_txs_gas_used_per_tx<P: JsonRpcClient + 'static>(
#     block: &Block<Transaction>,
#     fetcher: Arc<Fetcher<P>>,
# ) -> Result<Vec<u32>, CollectError> {
#     let mut tasks = Vec::new();
#     for tx in &block.transactions {
#         let tx_clone = tx.hash;
#         let fetcher = fetcher.clone();
#         let task = task::spawn(async move {
#             match fetcher.get_transaction_receipt(tx_clone).await? {
#                 Some(receipt) => Ok(receipt.gas_used),
#                 None => Err(CollectError::CollectError("could not find tx receipt".to_string())),
#             }
#         });
#         tasks.push(task);
#     }

#     let mut gas_used: Vec<u32> = Vec::new();
#     for task in tasks {
#         match task.await {
#             Ok(Ok(Some(value))) => gas_used.push(value.as_u32()),
#             _ => return Err(CollectError::CollectError("gas_used not available from node".into())),
#         }
#     }

#     Ok(gas_used)
# }
