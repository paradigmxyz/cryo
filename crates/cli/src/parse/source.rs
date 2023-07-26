use std::env;

use ethers::prelude::*;
use governor::{Quota, RateLimiter};
use polars::prelude::*;
use std::num::NonZeroU32;

use cryo_freeze::{ParseError, Source};

use crate::args::Args;

pub(crate) async fn parse_source(args: &Args) -> Result<Source, ParseError> {
    // parse network info
    let rpc_url = parse_rpc_url(args);
    let provider = Provider::<Http>::try_from(rpc_url)
        .map_err(|_e| ParseError::ParseError("could not connect to provider".to_string()))?;
    let chain_id = provider
        .get_chainid()
        .await
        .map_err(|_e| ParseError::ParseError("could not connect to provider".to_string()))?
        .as_u64();

    let rate_limiter = match args.requests_per_second {
        Some(rate_limit) => match NonZeroU32::new(rate_limit) {
            Some(value) => {
                let quota = Quota::per_second(value);
                Some(Arc::new(RateLimiter::direct(quota)))
            }
            _ => None,
        },
        None => None,
    };

    // process concurrency info
    let max_concurrent_requests = args.max_concurrent_requests.unwrap_or(100);
    let max_concurrent_chunks = args.max_concurrent_chunks.unwrap_or(3);

    let semaphore = tokio::sync::Semaphore::new(max_concurrent_requests as usize);
    let semaphore = Some(Arc::new(semaphore));

    let output = Source {
        provider: Arc::new(provider),
        chain_id,
        semaphore,
        rate_limiter,
        inner_request_size: args.inner_request_size,
        max_concurrent_chunks,
    };

    Ok(output)
}

fn parse_rpc_url(args: &Args) -> String {
    let mut url = match &args.rpc {
        Some(url) => url.clone(),
        _ => match env::var("ETH_RPC_URL") {
            Ok(url) => url,
            Err(_e) => {
                println!("must provide --rpc or set ETH_RPC_URL");
                std::process::exit(0);
            }
        },
    };
    if !url.starts_with("http") {
        url = "http://".to_string() + url.as_str();
    };
    url
}
