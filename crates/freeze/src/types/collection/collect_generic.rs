use crate::*;
use futures::Future;
use polars::prelude::*;
use std::collections::HashMap;
use tokio::{sync::mpsc, task};

// const TX_ERROR: &str = "datatype cannot collect by transaction";

/// collect data by block
pub async fn collect_by_block(
    datatype: MetaDatatype,
    partition: Partition,
    source: Source,
    schemas: HashMap<Datatype, Table>,
) -> Result<HashMap<Datatype, DataFrame>, CollectError> {
    let task = match datatype {
        MetaDatatype::Scalar(datatype) => match datatype {
            // Datatype::BalanceDiffs => BalanceDiffs::collect_by_block(partition, source, &schemas),
            // Datatype::Balances => Balances::collect_by_block(partition, source, &schemas),
            // Datatype::Blocks => Blocks::collect_by_block(partition, source, &schemas),
            // Datatype::CodeDiffs => CodeDiffs::collect_by_block(partition, source, &schemas),
            // Datatype::Codes => Codes::collect_by_block(partition, source, &schemas),
            // Datatype::Contracts => Contracts::collect_by_block(partition, source, &schemas),
            // Datatype::Erc20Balances => Erc20Balances::collect_by_block(partition, source, &schemas),
            // Datatype::Erc20Metadata => Erc20Metadata::collect_by_block(partition, source, &schemas),
            // Datatype::Erc20Supplies => Erc20Supplies::collect_by_block(partition, source, &schemas),
            // Datatype::Erc20Transfers => {
            //     Erc20Transfers::collect_by_block(partition, source, &schemas)
            // }
            // Datatype::Erc721Metadata => {
            //     Erc721Metadata::collect_by_block(partition, source, &schemas)
            // }
            // Datatype::Erc721Transfers => {
            //     Erc721Transfers::collect_by_block(partition, source, &schemas)
            // }
            // Datatype::EthCalls => EthCalls::collect_by_block(partition, source, &schemas),
            // Datatype::Logs => Logs::collect_by_block(partition, source, &schemas),
            // Datatype::NonceDiffs => NonceDiffs::collect_by_block(partition, source, &schemas),
            // Datatype::Nonces => Nonces::collect_by_block(partition, source, &schemas),
            // Datatype::StorageDiffs => StorageDiffs::collect_by_block(partition, source, &schemas),
            // Datatype::Storages => Storages::collect_by_block(partition, source, &schemas),
            // Datatype::Traces => Traces::collect_by_block(partition, source, &schemas),
            // Datatype::TraceCalls => TraceCalls::collect_by_block(partition, source, &schemas),
            // Datatype::Transactions => Transactions::collect_by_block(partition, source, &schemas),
            // Datatype::TransactionAddresses => {
            //     TransactionAddresses::collect_by_block(partition, source, &schemas)
            // }
            Datatype::VmTraces => VmTraces::collect_by_block(partition, source, &schemas),
            // Datatype::NativeTransfers => {
            //     NativeTransfers::collect_by_block(partition, source, &schemas)
            // }
            _ => panic!(),
        },
        // MetaDatatype::Multi(datatype) => match datatype {
        //     MultiDatatype::BlocksAndTransactions => {
        //         BlocksAndTransactions::collect_by_block(partition, source, &schemas)
        //     }
        //     MultiDatatype::StateDiffs => Err(CollectError::CollectError(TX_ERROR.to_string()))?,
        // },
        _ => panic!(),
    };
    task.await
}

/// collect data by transaction
pub async fn collect_by_transaction(
    datatype: MetaDatatype,
    partition: Partition,
    source: Source,
    schemas: HashMap<Datatype, Table>,
) -> Result<HashMap<Datatype, DataFrame>, CollectError> {
    let task = match datatype {
        MetaDatatype::Scalar(datatype) => match datatype {
            // Datatype::BalanceDiffs => {
            //     BalanceDiffs::collect_by_transaction(partition, source, &schemas)
            // }
            // Datatype::Balances => Balances::collect_by_transaction(partition, source, &schemas),
            // Datatype::Blocks => Blocks::collect_by_transaction(partition, source, &schemas),
            // Datatype::CodeDiffs => CodeDiffs::collect_by_transaction(partition, source, &schemas),
            // Datatype::Codes => Codes::collect_by_transaction(partition, source, &schemas),
            // Datatype::Contracts => Contracts::collect_by_transaction(partition, source, &schemas),
            // Datatype::Erc20Balances => return Err(CollectError::CollectError(TX_ERROR.to_string())),
            // Datatype::Erc20Metadata => return Err(CollectError::CollectError(TX_ERROR.to_string())),
            // Datatype::Erc20Supplies => return Err(CollectError::CollectError(TX_ERROR.to_string())),
            // Datatype::Erc20Transfers => {
            //     Erc20Transfers::collect_by_transaction(partition, source, &schemas)
            // }
            // Datatype::Erc721Metadata => {
            //     return Err(CollectError::CollectError(TX_ERROR.to_string()))
            // }
            // Datatype::Erc721Transfers => {
            //     Erc721Transfers::collect_by_transaction(partition, source, &schemas)
            // }
            // Datatype::EthCalls => return Err(CollectError::CollectError(TX_ERROR.to_string())),
            // Datatype::Logs => Logs::collect_by_transaction(partition, source, &schemas),
            // Datatype::NonceDiffs => NonceDiffs::collect_by_transaction(partition, source, &schemas),
            // Datatype::Nonces => Nonces::collect_by_transaction(partition, source, &schemas),
            // Datatype::StorageDiffs => {
            //     StorageDiffs::collect_by_transaction(partition, source, &schemas)
            // }
            // Datatype::Storages => Storages::collect_by_transaction(partition, source, &schemas),
            // Datatype::Traces => Traces::collect_by_transaction(partition, source, &schemas),
            // Datatype::TraceCalls => return Err(CollectError::CollectError(TX_ERROR.to_string())),
            // Datatype::Transactions => {
            //     Transactions::collect_by_transaction(partition, source, &schemas)
            // }
            // Datatype::TransactionAddresses => {
            //     TransactionAddresses::collect_by_transaction(partition, source, &schemas)
            // }
            Datatype::VmTraces => VmTraces::collect_by_transaction(partition, source, &schemas),
            // Datatype::NativeTransfers => {
            //     NativeTransfers::collect_by_transaction(partition, source, &schemas)
            // }
            _ => panic!(),
        },
        // MetaDatatype::Multi(datatype) => match datatype {
            // MultiDatatype::BlocksAndTransactions => {
            //     Err(CollectError::CollectError(TX_ERROR.to_string()))?
            // }
            // MultiDatatype::StateDiffs => Err(CollectError::CollectError(TX_ERROR.to_string()))?,
        // },
        _ => panic!(),
    };
    task.await
}

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
    param_dims: Vec<ChunkDim>,
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
