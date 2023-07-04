use std::sync::Arc;

use crate::types::RateLimiter;
use ethers::prelude::*;
use tokio::sync::Semaphore;

pub struct FetchOpts {
    // pub provider: Provider<Http>,
    pub provider: Arc<Provider<Http>>,
    pub semaphore: Arc<Semaphore>,
    pub rate_limiter: Option<Arc<RateLimiter>>,
}
