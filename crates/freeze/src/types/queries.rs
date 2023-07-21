use std::collections::HashMap;

use ethers::prelude::*;

use crate::types::Chunk;
use crate::types::Datatype;
use crate::types::Table;

/// Query multiple data types
#[derive(Clone)]
pub struct SingleQuery {
    /// Schemas for each datatype to collect
    pub schemas: Table,
    /// Block chunks to collect
    pub chunks: Vec<Chunk>,
    /// Row filter
    pub row_filter: Option<RowFilter>,
}

/// Query multiple data types
#[derive(Clone)]
pub struct MultiQuery {
    /// Schemas for each datatype to collect
    pub schemas: HashMap<Datatype, Table>,
    /// Block chunks to collect
    pub chunks: Vec<Chunk>,
    /// Row filter
    pub row_filters: HashMap<Datatype, RowFilter>,
}

/// Options for fetching logs
#[derive(Clone)]
pub struct RowFilter {
    /// topics to filter for
    pub topics: [Option<ValueOrArray<Option<H256>>>; 4],
    /// address to filter for
    pub address: Option<ValueOrArray<H160>>,
}
