use crate::*;
use futures::Future;
use polars::prelude::*;
use std::collections::HashMap;
use tokio::{sync::mpsc, task};

/// collect single partition
pub async fn collect_partition(
    time_dimension: TimeDimension,
    datatype: MetaDatatype,
    partition: Partition,
    source: Arc<Source>,
    schemas: HashMap<Datatype, Table>,
) -> Result<HashMap<Datatype, DataFrame>, CollectError> {
    match time_dimension {
        TimeDimension::Blocks => collect_by_block(datatype, partition, source, schemas).await,
        TimeDimension::Transactions => {
            collect_by_transaction(datatype, partition, source, schemas).await
        }
    }
}

/// fetch data for a given partition
pub async fn fetch_partition<F, Fut, T>(
    f_request: F,
    partition: Partition,
    source: Arc<Source>,
    inner_request_size: Option<u64>,
    schemas: HashMap<Datatype, Table>,
    sender: mpsc::Sender<Result<T, CollectError>>,
) -> Result<Vec<tokio::task::JoinHandle<Result<(), CollectError>>>, CollectError>
where
    F: Copy
        + Send
        + for<'a> Fn(Params, Arc<Source>, HashMap<Datatype, Table>) -> Fut
        + std::marker::Sync
        + 'static,
    Fut: Future<Output = Result<T, CollectError>> + Send + 'static,
    T: Send + 'static,
{
    let mut handles = Vec::new();
    for rpc_params in partition.param_sets(inner_request_size)?.into_iter() {
        let sender = sender.clone();
        let source = source.clone();
        let schemas = schemas.clone();
        let handle = task::spawn(async move {
            let result = f_request(rpc_params, source.clone(), schemas).await;
            match sender.send(result).await {
                Ok(_) => Ok(()),
                Err(_) => Err(CollectError::CollectError("tokio mpsc send failure".to_string())),
            }
        });
        handles.push(handle);
    }

    Ok(handles)
}

pub(crate) async fn join_partition_handles(
    handles: Vec<tokio::task::JoinHandle<Result<(), CollectError>>>,
) -> Result<(), CollectError> {
    let results: Vec<_> = futures::future::join_all(handles).await;
    for result in results {
        match result {
            Ok(Ok(())) => continue,
            Ok(Err(e)) => return Err(e),
            Err(join_err) => return Err(CollectError::TaskFailed(join_err)),
        }
    }
    Ok(())
}
