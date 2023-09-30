use crate::{ChunkDim, Datatype, MetaDatatype, Partition, Table};
use std::collections::HashMap;

/// Query
#[derive(Clone)]
pub struct Query {
    /// MetaDatatype
    pub datatypes: Vec<MetaDatatype>,
    /// Schemas for each subdatatype
    pub schemas: HashMap<Datatype, Table>,
    /// Time dimension
    pub time_dimension: TimeDimension,
    /// MetaChunks
    pub partitions: Vec<Partition>,
    /// Partitioning
    pub partitioned_by: Vec<ChunkDim>,
}

impl Query {
    /// total number of tasks needed to perform query
    pub fn n_tasks(&self) -> usize {
        self.datatypes.len() * self.partitions.len()
    }

    /// total number of outputs of query
    pub fn n_outputs(&self) -> usize {
        self.datatypes.iter().map(|x| x.datatypes().len()).sum::<usize>() * self.partitions.len()
    }
}

/// Time dimension for queries
#[derive(Clone)]
pub enum TimeDimension {
    /// Blocks
    Blocks,
    /// Transactions
    Transactions,
}
