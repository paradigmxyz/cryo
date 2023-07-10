/// aliases for external types
use governor::clock::DefaultClock;
use governor::{
    middleware::NoOpMiddleware,
    state::{direct::NotKeyed, InMemoryState},
};

/// RateLimiter based on governor crate
pub type RateLimiter = governor::RateLimiter<NotKeyed, InMemoryState, DefaultClock, NoOpMiddleware>;
