use std::collections::HashMap;

use polars::prelude::*;

use crate::{
    types::{CollectError, Datatype, MultiDatatype, Source, Table},
    Blocks, BlocksAndTransactions, CollectByBlock, CollectByTransaction, Logs, MetaDatatype,
    Partition, Query, TimeDimension, Traces, Transactions,
};

const TX_ERROR: &str = "datatype cannot collect by transaction";

/// collect single dataframe
pub async fn collect(query: Query, source: Source) -> Result<DataFrame, CollectError> {
    let datatype = if query.datatypes.len() != 1 { panic!() } else { query.datatypes[0].clone() };
    let partition =
        if query.partitions.len() != 1 { panic!() } else { query.partitions[0].clone() };
    let results =
        collect_partition(query.time_dimension, datatype, partition, source, query.schemas).await?;
    if results.len() > 1 {
        panic!("collect() only returns single dataframes")
    } else {
        Ok(results.into_iter().next().map(|(_, v)| v).unwrap())
    }
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

/// collect data by block
pub async fn collect_by_block(
    datatype: MetaDatatype,
    partition: Partition,
    source: Source,
    schemas: HashMap<Datatype, Table>,
) -> Result<HashMap<Datatype, DataFrame>, CollectError> {
    let task = match datatype {
        MetaDatatype::Scalar(datatype) => match datatype {
            Datatype::Blocks => Blocks::collect_by_block(partition, source, &schemas),
            Datatype::Logs => Logs::collect_by_block(partition, source, &schemas),
            Datatype::Traces => Traces::collect_by_block(partition, source, &schemas),
            Datatype::Transactions => Transactions::collect_by_block(partition, source, &schemas),
            _ => panic!(),
        },
        MetaDatatype::Multi(datatype) => match datatype {
            MultiDatatype::BlocksAndTransactions => {
                BlocksAndTransactions::collect_by_block(partition, source, &schemas)
            }
            MultiDatatype::StateDiffs => {
                panic!();
                // StateDiffs::collect_by_block(partition, source, schema),
            }
        },
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
            Datatype::Blocks => Blocks::collect_by_transaction(partition, source, &schemas),
            Datatype::Logs => Logs::collect_by_transaction(partition, source, &schemas),
            Datatype::Traces => Traces::collect_by_transaction(partition, source, &schemas),
            Datatype::Transactions => {
                Transactions::collect_by_transaction(partition, source, &schemas)
            }
            _ => panic!(),
        },
        MetaDatatype::Multi(datatype) => match datatype {
            MultiDatatype::BlocksAndTransactions => {
                Err(CollectError::CollectError(TX_ERROR.to_string()))?
            }
            MultiDatatype::StateDiffs => Err(CollectError::CollectError(TX_ERROR.to_string()))?,
        },
    };
    task.await
}
