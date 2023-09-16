use super::partitions;
use crate::{CollectError, Fetcher, Source};
use ethers::prelude::*;
use futures::Future;
use tokio::{sync::mpsc, task};

/// fetch data for a given partition
pub async fn fetch_partition<F, Fut, T>(
    f_request: F,
    meta_chunk: partitions::MetaChunk,
    source: Source,
    param_dims: Vec<partitions::ChunkDim>,
    sender: mpsc::Sender<Result<T, CollectError>>,
) -> Result<(), CollectError>
where
    F: for<'a> Fn(partitions::RpcParams, Source) -> Fut,
    Fut: Future<Output = Result<T, CollectError>> + Send + 'static,
    T: Send + 'static,
{
    for rpc_params in meta_chunk.param_sets(param_dims).into_iter() {
        let result = f_request(rpc_params, source.clone()).await;
        match sender.send(result).await {
            Ok(_) => {}
            Err(tokio::sync::mpsc::error::SendError(_e)) => {
                return Err(CollectError::CollectError("mpsc channel failed".to_string()))
            }
        };
    }
    Ok(())
}

/// example
pub async fn example_other() {
    // given beforehand
    let meta_chunk = partitions::MetaChunk::default();
    let fetcher = Fetcher {
        provider: Provider::<Http>::try_from("hi").unwrap(),
        semaphore: None,
        rate_limiter: None,
    };
    let source = Source {
        fetcher: std::sync::Arc::new(fetcher),
        chain_id: 1,
        inner_request_size: 100,
        max_concurrent_chunks: 32,
    };
    let param_dims = vec![];

    // inside collect function
    let (sender, _receiver) = mpsc::channel(1);
    let result = fetch_partition(fetch_transaction, meta_chunk, source, param_dims, sender).await;
}

async fn fetch_transaction(
    request: partitions::RpcParams,
    source: Source,
) -> Result<u64, CollectError> {
    // async fn fetch_transaction(request: RpcParams) -> Result<u64, CollectError> {
    println!("hi");
    Ok(23)
}
