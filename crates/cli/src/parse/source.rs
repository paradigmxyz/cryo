use std::env;

use ethers::prelude::*;
use governor::{Quota, RateLimiter};
use polars::prelude::*;
use std::num::NonZeroU32;

use cryo_freeze::{Fetcher, ParseError, Source};

use crate::args::Args;

pub(crate) async fn parse_source(args: &Args) -> Result<Source, ParseError> {
    // parse network info
    let provider = parse_rpc_url(args).await;
    let chain_id: u64;

    let rate_limiter = match args.requests_per_second {
        Some(rate_limit) => match NonZeroU32::new(rate_limit) {
            Some(value) => {
                let quota = Quota::per_second(value);
                Some(RateLimiter::direct(quota))
            }
            _ => None,
        },
        None => None,
    };

    // process concurrency info
    let max_concurrent_requests = args.max_concurrent_requests.unwrap_or(100);
    let max_concurrent_chunks = args.max_concurrent_chunks.unwrap_or(3);

    let semaphore = tokio::sync::Semaphore::new(max_concurrent_requests as usize);
    let semaphore = Some(semaphore);

    match provider {
        RpcProvider::Http(provider) => {
            chain_id = provider.get_chainid().await.map_err(ParseError::ProviderError)?.as_u64();
            let fetcher = Fetcher { provider, semaphore, rate_limiter };
            let output = Source {
                fetcher: Arc::new(fetcher),
                chain_id,
                inner_request_size: args.inner_request_size,
                max_concurrent_chunks,
            };

            Ok(output)
        }
        RpcProvider::Ws(provider) => {
            chain_id = provider.get_chainid().await.map_err(ParseError::ProviderError)?.as_u64();
            let fetcher = Fetcher { provider, semaphore, rate_limiter };
            let output = Source {
                fetcher: Arc::new(fetcher),
                chain_id,
                inner_request_size: args.inner_request_size,
                max_concurrent_chunks,
            };

            Ok(output)
        }
    }
}

async fn parse_rpc_url(args: &Args) -> RpcProvider {
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
    if url.starts_with("wss") {
        let ws_provider = Provider::<Ws>::connect(url)
            .await
            .map_err(|_e| ParseError::ParseError("could not connect to ws_provider".to_string()))
            .unwrap();
        return RpcProvider::Ws(ws_provider)
    } else {
        if !url.starts_with("http") {
            url = "http://".to_string() + url.as_str();
        }
        let http_provider =
            Provider::<RetryClient<Http>>::new_client(&url, args.max_retries, args.initial_backoff)
                .map_err(|_e| ParseError::ParseError("could not connect to provider".to_string()))
                .unwrap();
        return RpcProvider::Http(http_provider)
    }
}

enum RpcProvider {
    Http(Provider<RetryClient<Http>>),
    Ws(Provider<Ws>),
}
