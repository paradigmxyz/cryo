use std::{
    collections::{HashMap, HashSet},
    path::{Path, PathBuf},
};

use ethers::prelude::*;

use crate::{
    types::{Chunk, Datatype, Table},
    CollectError, FileOutput, FreezeError,
};

/// Query multiple data types
#[derive(Clone)]
pub struct SingleQuery {
    /// Datatype for query
    pub datatype: Datatype,
    /// Schemas for each datatype to collect
    pub schema: Table,
    /// Block chunks to collect
    pub chunks: Vec<(Chunk, Option<String>)>,
    /// Row filter
    pub row_filter: Option<RowFilter>,
}

/// Query multiple data types
#[derive(Clone)]
pub struct MultiQuery {
    /// Schemas for each datatype to collect
    pub schemas: HashMap<Datatype, Table>,
    /// Block chunks to collect
    pub chunks: Vec<(Chunk, Option<String>)>,
    /// Row filter
    pub row_filters: HashMap<Datatype, RowFilter>,
}

impl MultiQuery {
    /// get number of chunks that have not yet been collected
    pub fn get_n_chunks_remaining(&self, sink: &FileOutput) -> Result<u64, FreezeError> {
        let actual_files: HashSet<PathBuf> = list_files(&sink.output_dir)
            .map_err(|_e| {
                FreezeError::CollectError(CollectError::CollectError(
                    "could not list files in output dir".to_string(),
                ))
            })?
            .into_iter()
            .collect();
        let mut n_chunks_remaining: u64 = 0;
        for (chunk, chunk_label) in &self.chunks {
            let chunk_files = chunk.filepaths(self.schemas.keys().collect(), sink, chunk_label)?;
            if !chunk_files.values().all(|file| actual_files.contains(file)) {
                n_chunks_remaining += 1;
            }
        }
        Ok(n_chunks_remaining)
    }
}

fn list_files(dir: &Path) -> Result<Vec<PathBuf>, std::io::Error> {
    let mut file_list = Vec::new();
    for entry in std::fs::read_dir(dir)? {
        let entry = entry?;
        file_list.push(entry.path());
    }
    Ok(file_list)
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
