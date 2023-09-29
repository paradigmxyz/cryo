use crate::{ChunkDim, CollectError, Datatype, Partition, RpcParams, Source, Table};
use futures::Future;
use std::collections::HashMap;
use tokio::{sync::mpsc, task};

/// fetch data for a given partition
pub async fn fetch_partition<F, Fut, T>(
    f_request: F,
    partition: Partition,
    source: Source,
    schemas: HashMap<Datatype, Table>,
    param_dims: Vec<ChunkDim>,
    sender: mpsc::Sender<Result<T, CollectError>>,
) -> Result<(), CollectError>
where
    F: Copy
        + Send
        + for<'a> Fn(RpcParams, Source, HashMap<Datatype, Table>) -> Fut
        + std::marker::Sync
        + 'static,
    Fut: Future<Output = Result<T, CollectError>> + Send + 'static,
    T: Send + 'static,
{
    let mut handles = Vec::new();
    for rpc_params in partition.param_sets(param_dims).into_iter() {
        let sender = sender.clone();
        let source = source.clone();
        let schemas = schemas.clone();
        let handle = task::spawn(async move {
            let result = f_request(rpc_params, source.clone(), schemas).await;
            sender.send(result).await.expect("tokio mpsc send failure");
        });
        handles.push(handle);
    }
    Ok(())
}
