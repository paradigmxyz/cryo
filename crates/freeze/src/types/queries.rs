use std::collections::HashMap;

use ethers::prelude::*;

use crate::types::{Chunk, Datatype, Table};

/// Query multiple data types
#[derive(Clone)]
pub struct SingleQuery {
    /// Datatype for query
    pub datatype: Datatype,
    /// Schemas for each datatype to collect
    pub schema: Table,
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

impl From<MultiQuery> for SingleQuery {
    fn from(query: MultiQuery) -> Self {
        let (datatype, schema) = match query.schemas.len() {
            0 => panic!("bad query, needs 1 datatype"),
            1 => {
                let datatype_schema =
                    query.schemas.iter().next().expect("Expected at least one schema");
                (*datatype_schema.0, datatype_schema.1.clone())
            }
            _ => panic!("bad query, needs 1 datatype"),
        };
        let row_filter = query.row_filters.get(&datatype).cloned();
        SingleQuery { datatype, schema, chunks: query.chunks, row_filter }
    }
}
