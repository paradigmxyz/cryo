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
    } else if rpc_url.ends_with(".ipc") {
        let provider: Provider<Ipc> = Provider::connect_ipc(&rpc_url).await.map_err(|_| {
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
            max_concurrent_requests: args.max_concurrent_requests,
            max_requests_per_second: args.requests_per_second.map(|x| x as u64),
            max_retries: Some(args.max_retries),
            initial_backoff: Some(args.initial_backoff),
        },
    };

    Ok(output)
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
                eprintln!("Could not load MESC data: {}", e);
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
    if !url.starts_with("http") & !url.starts_with("ws") & !url.ends_with(".ipc") {
        Ok("http://".to_string() + url.as_str())
    } else {
        Ok(url)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use thousands::Separable;

    async fn setup_source(max_concurrent_requests: u64) -> Source {
        let rpc_url = match crate::parse::source::parse_rpc_url(&Args::default()) {
            Ok(url) => url,
            Err(_) => std::process::exit(0),
        };
        let max_retry = 5;
        let initial_backoff = 500;
        let max_concurrent_requests = max_concurrent_requests;
        let provider =
            Provider::<RetryClient<Http>>::new_client(&rpc_url, max_retry, initial_backoff)
                .map_err(|_e| ParseError::ParseError("could not connect to provider".to_string()))
                .unwrap();

        let quota = Quota::per_second(NonZeroU32::new(15).unwrap())
            .allow_burst(NonZeroU32::new(1).unwrap());
        let rate_limiter = Some(RateLimiter::direct(quota));
        let semaphore = tokio::sync::Semaphore::new(max_concurrent_requests as usize);

        Source {
            provider: provider.into(),
            semaphore: Arc::new(Some(semaphore)),
            rate_limiter: Arc::new(rate_limiter),
            chain_id: 1,
            inner_request_size: 1,
            max_concurrent_chunks: None,
            rpc_url: "".to_string(),
            labels: SourceLabels {
                max_concurrent_requests: Some(max_concurrent_requests),
                ..SourceLabels::default()
            },
        }
    }

    // modified freeze::types::summaries::print_bullet_indent(), returned as a string and removed formatting dependencies.
    fn return_bullet_indent_no_formatting<A: AsRef<str>, B: AsRef<str>>(key: A, value: B, indent: usize) -> String {
        let bullet_str = "- ";
        let key_str = key.as_ref();
        let value_str = value.as_ref();
        let colon_str = ": ";
        format!("{}{}{}{}{}", " ".repeat(indent), bullet_str, key_str, colon_str, value_str)
    }

    #[tokio::test]
    async fn test_max_concurrent_requests_printing_some() {
        let source: Source = setup_source(1337).await;

        let output = return_bullet_indent_no_formatting(
            "max concurrent requests",
            source.labels.max_concurrent_requests.unwrap().separate_with_commas(),
            4,
        );

        let expected = format!("    - max concurrent requests: 1,337");

        assert_eq!(output, expected);
    }
}