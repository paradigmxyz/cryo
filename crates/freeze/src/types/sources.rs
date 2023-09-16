use std::sync::Arc;

use ethers::prelude::*;
use governor::{
    clock::DefaultClock,
    middleware::NoOpMiddleware,
    state::{direct::NotKeyed, InMemoryState},
};
use tokio::sync::{AcquireError, Semaphore, SemaphorePermit};

use crate::CollectError;

/// RateLimiter based on governor crate
pub type RateLimiter = governor::RateLimiter<NotKeyed, InMemoryState, DefaultClock, NoOpMiddleware>;

/// Options for fetching data from node
#[derive(Clone)]
pub struct Source {
    /// Shared provider for rpc data
    pub fetcher: Arc<Fetcher<RetryClient<Http>>>,
    /// chain_id of network
    pub chain_id: u64,
    /// number of blocks per log request
    pub inner_request_size: u64,
    /// Maximum chunks collected concurrently
    pub max_concurrent_chunks: u64,
}

/// Wrapper over `Provider<P>` that adds concurrency and rate limiting controls
pub struct Fetcher<P> {
    /// provider data source
    pub provider: Provider<P>,
    /// semaphore for controlling concurrency
    pub semaphore: Option<Semaphore>,
    /// rate limiter for controlling request rate
    pub rate_limiter: Option<RateLimiter>,
}

type Result<T> = ::core::result::Result<T, CollectError>;

impl<P: JsonRpcClient> Fetcher<P> {
    /// Returns an array (possibly empty) of logs that match the filter
    pub async fn get_logs(&self, filter: &Filter) -> Result<Vec<Log>> {
        let _permit = self.permit_request().await;
        Self::map_err(self.provider.get_logs(filter).await)
    }

    /// Replays all transactions in a block returning the requested traces for each transaction
    pub async fn trace_replay_block_transactions(
        &self,
        block: BlockNumber,
        trace_types: Vec<TraceType>,
    ) -> Result<Vec<BlockTrace>> {
        let _permit = self.permit_request().await;
        Self::map_err(self.provider.trace_replay_block_transactions(block, trace_types).await)
    }

    /// Replays a transaction, returning the traces
    pub async fn trace_replay_transaction(
        &self,
        tx_hash: TxHash,
        trace_types: Vec<TraceType>,
    ) -> Result<BlockTrace> {
        let _permit = self.permit_request().await;
        Self::map_err(self.provider.trace_replay_transaction(tx_hash, trace_types).await)
    }

    /// Gets the transaction with transaction_hash
    pub async fn get_transaction(&self, tx_hash: TxHash) -> Result<Option<Transaction>> {
        let _permit = self.permit_request().await;
        Self::map_err(self.provider.get_transaction(tx_hash).await)
    }

    /// Gets the transaction receipt with transaction_hash
    pub async fn get_transaction_receipt(
        &self,
        tx_hash: TxHash,
    ) -> Result<Option<TransactionReceipt>> {
        let _permit = self.permit_request().await;
        Self::map_err(self.provider.get_transaction_receipt(tx_hash).await)
    }

    /// Gets the block at `block_num` (transaction hashes only)
    pub async fn get_block(&self, block_num: u64) -> Result<Option<Block<TxHash>>> {
        let _permit = self.permit_request().await;
        Self::map_err(self.provider.get_block(block_num).await)
    }

    /// Gets the block at `block_num` (full transactions included)
    pub async fn get_block_with_txs(&self, block_num: u64) -> Result<Option<Block<Transaction>>> {
        let _permit = self.permit_request().await;
        Self::map_err(self.provider.get_block_with_txs(block_num).await)
    }

    /// Returns all receipts for a block.
    pub async fn get_block_receipts(&self, block_num: u64) -> Result<Vec<TransactionReceipt>> {
        let _permit = self.permit_request().await;
        Self::map_err(self.provider.get_block_receipts(block_num).await)
    }

    /// Returns traces created at given block
    pub async fn trace_block(&self, block_num: BlockNumber) -> Result<Vec<Trace>> {
        let _permit = self.permit_request().await;
        Self::map_err(self.provider.trace_block(block_num).await)
    }

    /// Returns all traces of a given transaction
    pub async fn trace_transaction(&self, tx_hash: TxHash) -> Result<Vec<Trace>> {
        let _permit = self.permit_request().await;
        self.provider.trace_transaction(tx_hash).await.map_err(CollectError::ProviderError)
    }

    /// Return output data of a contract call
    pub async fn call(
        &self,
        transaction: TransactionRequest,
        block_number: BlockNumber,
    ) -> Result<Bytes> {
        let _permit = self.permit_request().await;
        self.provider
            .call(&transaction.into(), Some(block_number.into()))
            .await
            .map_err(CollectError::ProviderError)
    }

    /// Returns traces for given call data
    pub async fn trace_call(
        &self,
        transaction: TransactionRequest,
        trace_type: Vec<TraceType>,
        block_number: Option<BlockNumber>,
    ) -> Result<BlockTrace> {
        let _permit = self.permit_request().await;
        self.provider
            .trace_call(transaction, trace_type, block_number)
            .await
            .map_err(CollectError::ProviderError)
    }

    /// Get nonce of address
    pub async fn get_transaction_count(
        &self,
        address: H160,
        block_number: BlockNumber,
    ) -> Result<U256> {
        let _permit = self.permit_request().await;
        self.provider
            .get_transaction_count(address, Some(block_number.into()))
            .await
            .map_err(CollectError::ProviderError)
    }

    /// Get code at address
    pub async fn get_balance(&self, address: H160, block_number: BlockNumber) -> Result<U256> {
        let _permit = self.permit_request().await;
        self.provider
            .get_balance(address, Some(block_number.into()))
            .await
            .map_err(CollectError::ProviderError)
    }

    /// Get code at address
    pub async fn get_code(&self, address: H160, block_number: BlockNumber) -> Result<Bytes> {
        let _permit = self.permit_request().await;
        self.provider
            .get_code(address, Some(block_number.into()))
            .await
            .map_err(CollectError::ProviderError)
    }

    /// Get stored data at given location
    pub async fn get_storage_at(
        &self,
        address: H160,
        slot: H256,
        block_number: BlockNumber,
    ) -> Result<H256> {
        let _permit = self.permit_request().await;
        self.provider
            .get_storage_at(address, slot, Some(block_number.into()))
            .await
            .map_err(CollectError::ProviderError)
    }

    /// Get the block number
    pub async fn get_block_number(&self) -> Result<U64> {
        Self::map_err(self.provider.get_block_number().await)
    }

    async fn permit_request(
        &self,
    ) -> Option<::core::result::Result<SemaphorePermit<'_>, AcquireError>> {
        let permit = match &self.semaphore {
            Some(semaphore) => Some(semaphore.acquire().await),
            _ => None,
        };
        if let Some(limiter) = &self.rate_limiter {
            limiter.until_ready().await;
        }
        permit
    }

    fn map_err<T>(res: ::core::result::Result<T, ProviderError>) -> Result<T> {
        res.map_err(CollectError::ProviderError)
    }
}

// impl Source {
//     /// create Source for an individual chunk
//     pub fn build_source(&self) -> Source {
//         let sem = Arc::new(tokio::sync::Semaphore::new(
//             self.max_concurrent_blocks as usize,
//         ));
//         Source {
//             provider: Arc::clone(&self.provider),
//             rate_limiter: self.rate_limiter.as_ref().map(Arc::clone),
//             semaphore: sem,
//             chain_id: self.chain_id,
//             inner_request_size: self.inner_request_size,
//             max
//         }
//     }
// }

// pub struct SourceBuilder {
//     provider: Option<Arc<Provider<Http>>>,
//     semaphore: Option<Arc<Semaphore>>,
//     rate_limiter: Option<Arc<RateLimiter>>,
//     chain_id: Option<u64>,
//     inner_request_size: Option<u64>,
//     max_concurrent_chunks: Option<u64>,
// }

// impl SourceBuilder {
//     pub fn new() -> SourceBuilder {
//         SourceBuilder {
//             provider: None,
//             semaphore: None,
//             rate_limiter: None,
//             chain_id: None,
//             inner_request_size: None,
//             max_concurrent_chunks: None,
//         }
//     }

//     pub fn provider(mut self, provider: Arc<Provider<Http>>) -> Self {
//         self.provider = Some(provider);
//         self
//     }

//     pub fn semaphore(mut self, semaphore: Arc<Semaphore>) -> Self {
//         self.semaphore = Some(semaphore);
//         self
//     }

//     pub fn rate_limiter(mut self, rate_limiter: Arc<RateLimiter>) -> Self {
//         self.rate_limiter = Some(rate_limiter);
//         self
//     }

//     pub fn chain_id(mut self, chain_id: u64) -> Self {
//         self.chain_id = Some(chain_id);
//         self
//     }

//     pub fn inner_request_size(mut self, inner_request_size: u64) -> Self {
//         self.inner_request_size = Some(inner_request_size);
//         self
//     }

//     pub fn max_concurrent_chunks(mut self, max_concurrent_chunks: u64) -> Self {
//         self.max_concurrent_chunks = Some(max_concurrent_chunks);
//         self
//     }

//     pub fn build(self) -> Result<Source, &'static str> {
//         if let (
//             Some(provider),
//             Some(semaphore),
//             Some(chain_id),
//             Some(inner_request_size),
//             Some(max_concurrent_chunks),
//         ) = ( self.provider, self.semaphore, self.chain_id, self.inner_request_size,
//           self.max_concurrent_chunks,
//         ) { Ok(Source { provider, semaphore, rate_limiter: self.rate_limiter, chain_id,
//           inner_request_size, max_concurrent_chunks, })
//         } else {
//             Err("Cannot build Source. Missing fields.")
//         }
//     }
// }
