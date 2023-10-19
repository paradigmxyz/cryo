use std::env;

use ethers::prelude::*;
use governor::{Quota, RateLimiter};
use polars::prelude::*;
use std::num::NonZeroU32;

use cryo_freeze::{Fetcher, ParseError, Source, SourceLabels};

use crate::args::Args;

pub(crate) async fn parse_source(args: &Args) -> Result<Source, ParseError> {
    // parse network info
    let rpc_url = parse_rpc_url(args);
    let provider =
        Provider::<RetryClient<Http>>::new_client(&rpc_url, args.max_retries, args.initial_backoff)
            .map_err(|_e| ParseError::ParseError("could not connect to provider".to_string()))?;
    let chain_id = provider.get_chainid().await.map_err(ParseError::ProviderError)?.as_u64();

    let rate_limiter = match args.requests_per_second {
        Some(rate_limit) => match (NonZeroU32::new(1), NonZeroU32::new(rate_limit)) {
            (Some(one), Some(value)) => {
                let quota = Quota::per_second(value).allow_burst(one);
                Some(RateLimiter::direct(quota))
            }
            _ => None,
        },
        None => None,
    };

    // process concurrency info
    let max_concurrent_requests = args.max_concurrent_requests.unwrap_or(100);
    let max_concurrent_chunks = match args.max_concurrent_chunks {
        Some(0) => None,
        Some(max) => Some(max),
        None => Some(4),
    };

    let semaphore = tokio::sync::Semaphore::new(max_concurrent_requests as usize);
    let semaphore = Some(semaphore);

    let fetcher = Fetcher { provider, semaphore, rate_limiter };
    let output = Source {
        fetcher: Arc::new(fetcher),
        chain_id,
        inner_request_size: args.inner_request_size,
        max_concurrent_chunks,
        rpc_url,
        labels: SourceLabels {
            max_concurrent_requests: args.requests_per_second.map(|x| x as u64),
            max_requests_per_second: args.requests_per_second.map(|x| x as u64),
            max_retries: Some(args.max_retries),
            initial_backoff: Some(args.initial_backoff),
        },
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
