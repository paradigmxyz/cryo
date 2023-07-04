use std::sync::Arc;

use tokio::sync::Semaphore;
use ethers::prelude::*;
use crate::types::RateLimiter;

pub struct FetchOpts {
    // pub provider: Provider<Http>,
    pub provider: Arc<Provider<Http>>,
    pub semaphore: Arc<Semaphore>,
    pub rate_limiter: Option<Arc<RateLimiter>>,
}

