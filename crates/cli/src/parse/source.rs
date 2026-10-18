use std::env;

use crate::args::Args;
use cryo_freeze::{ParseError, Source, SourceLabels};
use governor::{Quota, RateLimiter};
use std::num::NonZeroU32;

pub(crate) async fn parse_source(args: &Args) -> Result<Source, ParseError> {
    // parse network info
    let rpc_url = parse_rpc_url(args)?;
    // capture jwt token from args or environment
    let jwt = match &args.jwt {
        Some(v) => Some(v.clone()),
        None => std::env::var("ETH_RPC_JWT").ok(),
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

    let labels = SourceLabels {
        max_concurrent_requests: args.max_concurrent_requests,
        max_requests_per_second: args.requests_per_second.map(|x| x as u64),
        max_retries: Some(args.max_retries),
        initial_backoff: Some(args.initial_backoff),
    };

    let source = Source::builder()
        .rpc_url(rpc_url.clone())
        .inner_request_size(args.inner_request_size)
        .max_concurrent_chunks(max_concurrent_chunks)
        .semaphore(Some(semaphore))
        .rate_limiter(rate_limiter)
        .jwt(jwt)
        .labels(labels)
        .retry_policy(args.max_retries, args.initial_backoff, args.compute_units_per_second)
        .build()
        .await
        .map_err(|e| ParseError::ParseError(format!("failed to build Source: {e}")))?;

    Ok(source)
}

pub(crate) fn parse_rpc_url(args: &Args) -> Result<String, ParseError> {
    // get MESC url
    let mesc_url = if mesc::is_mesc_enabled() {
        let endpoint = match &args.rpc {
            Some(url) => mesc::get_endpoint_by_query(url, Some("cryo")),
            None => mesc::get_default_endpoint(Some("cryo")),
        };
        match endpoint {
            Ok(endpoint) => endpoint.map(|endpoint| endpoint.url),
            Err(e) => {
                error!("Could not load MESC data: {e}");
                None
            }
        }
    } else {
        None
    };

    // use ETH_RPC_URL if no MESC url found
    let url = if let Some(url) = mesc_url {
        url
    } else if let Some(url) = &args.rpc {
        url.clone()
    } else if let Ok(url) = env::var("ETH_RPC_URL") {
        url
    } else {
        let message = "must provide --rpc or setup MESC or set ETH_RPC_URL";
        return Err(ParseError::ParseError(message.to_string()))
    };

    // prepend http or https if need be
    Ok(url)
}
