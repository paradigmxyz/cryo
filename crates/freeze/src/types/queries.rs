use crate::{CollectError, Datatype, Dim, MetaDatatype, Partition, Table};
use std::collections::{HashMap, HashSet};

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
    pub partitioned_by: Vec<Dim>,
    /// Labels (these are non-functional)
    pub labels: QueryLabels,
}

/// query labels (non-functional)
#[derive(Clone)]
pub struct QueryLabels {
    /// align
    pub align: bool,
    /// reorg buffer
    pub reorg_buffer: u64,
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

    /// check that query is valid
    pub fn is_valid(&self) -> Result<(), CollectError> {
        // check that required parameters are present
        let mut all_datatypes = std::collections::HashSet::new();
        for datatype in self.datatypes.iter() {
            all_datatypes.extend(datatype.datatypes())
        }
        let mut requirements: HashSet<Dim> = HashSet::new();
        for datatype in all_datatypes.iter() {
            for dim in datatype.required_parameters() {
                requirements.insert(dim);
            }
        }
        for partition in self.partitions.iter() {
            let partition_dims = partition.dims().into_iter().collect();
            if !requirements.is_subset(&partition_dims) {
                let missing: Vec<_> =
                    requirements.difference(&partition_dims).map(|x| x.to_string()).collect();
                return Err(CollectError::CollectError(format!(
                    "need to specify {}",
                    missing.join(", ")
                )))
            }
        }
        Ok(())
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
