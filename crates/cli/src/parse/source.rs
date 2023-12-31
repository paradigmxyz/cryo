use std::env;

use crate::args::Args;
use cryo_freeze::{sources::ProviderWrapper, ParseError, Source, SourceLabels};
use ethers::prelude::*;
use governor::{Quota, RateLimiter};
use polars::prelude::*;
use std::num::NonZeroU32;

pub(crate) async fn parse_source(args: &Args) -> Result<Source, ParseError> {
    // parse network info
    let rpc_url = parse_rpc_url(args)?;
    let (provider, chain_id): (ProviderWrapper, u64) = if rpc_url.starts_with("http") {
        let provider = Provider::<RetryClient<Http>>::new_client(
            &rpc_url,
            args.max_retries,
            args.initial_backoff,
        )
        .map_err(|_e| ParseError::ParseError("could not connect to provider".to_string()))?;
        let chain_id = provider.get_chainid().await.map_err(ParseError::ProviderError)?.as_u64();
        (provider.into(), chain_id)
    } else if rpc_url.starts_with("ws") {
        let provider = Provider::<Ws>::connect(&rpc_url).await.map_err(|_| {
            ParseError::ParseError("could not instantiate HTTP Provider".to_string())
        })?;
        let chain_id = provider.get_chainid().await.map_err(ParseError::ProviderError)?.as_u64();
        (provider.into(), chain_id)
    } else {
        return Err(ParseError::ParseError(format!("invalid rpc url: {}", rpc_url)));
    };

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
    let semaphore = Arc::new(Some(semaphore));

    let output = Source {
        chain_id,
        inner_request_size: args.inner_request_size,
        max_concurrent_chunks,
        semaphore,
        rate_limiter: rate_limiter.into(),
        rpc_url,
        provider,
        labels: SourceLabels {
            max_concurrent_requests: args.requests_per_second.map(|x| x as u64),
            max_requests_per_second: args.requests_per_second.map(|x| x as u64),
            max_retries: Some(args.max_retries),
            initial_backoff: Some(args.initial_backoff),
        },
    };

    Ok(output)
}

pub(crate) fn parse_rpc_url(args: &Args) -> Result<String, ParseError> {
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
    if !url.starts_with("http") & !url.starts_with("ws") {
        url = "http://".to_string() + url.as_str();
    };
    Ok(url)
}
