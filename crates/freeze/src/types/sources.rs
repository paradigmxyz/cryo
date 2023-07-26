use std::sync::Arc;

use ethers::prelude::*;
use governor::{
    clock::DefaultClock,
    middleware::NoOpMiddleware,
    state::{direct::NotKeyed, InMemoryState},
};
use tokio::sync::Semaphore;

/// RateLimiter based on governor crate
pub type RateLimiter = governor::RateLimiter<NotKeyed, InMemoryState, DefaultClock, NoOpMiddleware>;

/// Options for fetching data from node
#[derive(Clone)]
pub struct Source {
    /// provider data source
    pub provider: Arc<Provider<Http>>,
    /// semaphore for controlling concurrency
    pub semaphore: Option<Arc<Semaphore>>,
    /// rate limiter for controlling request rate
    pub rate_limiter: Option<Arc<RateLimiter>>,
    /// chain_id of network
    pub chain_id: u64,
    /// number of blocks per log request
    pub inner_request_size: u64,
    /// Maximum chunks collected concurrently
    pub max_concurrent_chunks: u64,
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
