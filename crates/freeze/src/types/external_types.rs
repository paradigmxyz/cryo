use governor::clock::DefaultClock;
use governor::middleware::NoOpMiddleware;
use governor::state::direct::NotKeyed;
use governor::state::InMemoryState;

pub type RateLimiter = governor::RateLimiter<NotKeyed, InMemoryState, DefaultClock, NoOpMiddleware>;
