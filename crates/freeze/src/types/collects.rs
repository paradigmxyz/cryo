use std::sync::Arc;

use crate::types::RateLimiter;
use ethers::prelude::*;
use tokio::sync::Semaphore;

/// Options for fetching data from node
pub struct FetchOpts {
    /// provider data source
    pub provider: Arc<Provider<Http>>,
    /// semaphore for controlling concurrency
    pub semaphore: Arc<Semaphore>,
    /// rate limiter for controlling request rate
    pub rate_limiter: Option<Arc<RateLimiter>>,
}

/// Options for fetching logs
#[derive(Clone)]
pub struct LogOpts {
    /// topics to filter for
    pub topics: [Option<ValueOrArray<Option<H256>>>; 4],
    /// address to filter for
    pub address: Option<ValueOrArray<H160>>,
    /// number of blocks per log request
    pub log_request_size: u64,
}
