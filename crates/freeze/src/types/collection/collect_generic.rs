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
    source: Source,
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
    source: Source,
    schemas: HashMap<Datatype, Table>,
    sender: mpsc::Sender<Result<T, CollectError>>,
) -> Result<(), CollectError>
where
    F: Copy
        + Send
        + for<'a> Fn(Params, Source, HashMap<Datatype, Table>) -> Fut
        + std::marker::Sync
        + 'static,
    Fut: Future<Output = Result<T, CollectError>> + Send + 'static,
    T: Send + 'static,
{
    let mut handles = Vec::new();
    for rpc_params in partition.param_sets()?.into_iter() {
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
