use std::{
    collections::{HashMap, HashSet},
    path::{Path, PathBuf},
};

use ethers::prelude::*;

use crate::{
    types::{Chunk, Datatype, Table},
    AddressChunk, CallDataChunk, CollectError, FileOutput, FreezeError, SlotChunk,
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
        if sink.overwrite {
            return Ok(self.chunks.len() as u64)
        };
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
#[derive(Clone, Default)]
pub struct RowFilter {
    /// topics to filter for
    pub topics: [Option<ValueOrArray<Option<H256>>>; 4],
    /// address to filter for
    pub address: Option<ValueOrArray<H160>>,
    /// address chunks to collect
    pub address_chunks: Option<Vec<AddressChunk>>,
    /// contract chunks to collect
    pub contract_chunks: Option<Vec<AddressChunk>>,
    /// to_address chunks to collect
    pub to_address_chunks: Option<Vec<AddressChunk>>,
    /// slot chunks to collect
    pub slot_chunks: Option<Vec<SlotChunk>>,
    /// call_data chunks to collect
    pub call_data_chunks: Option<Vec<CallDataChunk>>,
}

impl RowFilter {
    pub(crate) fn address_chunks(&self) -> Result<Vec<AddressChunk>, CollectError> {
        match &self.address_chunks {
            Some(address_chunks) => Ok(address_chunks.clone()),
            _ => Err(CollectError::CollectError("must specify --address".to_string())),
        }
    }

    pub(crate) fn contract_chunks(&self) -> Result<Vec<AddressChunk>, CollectError> {
        match &self.contract_chunks {
            Some(contract_chunks) => Ok(contract_chunks.clone()),
            _ => Err(CollectError::CollectError("must specify --contract".to_string())),
        }
    }

    pub(crate) fn to_address_chunks(&self) -> Result<Vec<AddressChunk>, CollectError> {
        match &self.to_address_chunks {
            Some(to_address_chunks) => Ok(to_address_chunks.clone()),
            _ => Err(CollectError::CollectError("must specify --to-address".to_string())),
        }
    }

    // pub(crate) fn from_address_chunks(&self) -> Result<Vec<AddressChunk>, CollectError> {
    //     match &self.from_address_chunks {
    //         Some(from_address_chunks) => Ok(from_address_chunks.clone()),
    //         _ => Err(CollectError::CollectError("must specify --from-address".to_string())),
    //     }
    // }

    pub(crate) fn slot_chunks(&self) -> Result<Vec<SlotChunk>, CollectError> {
        match &self.slot_chunks {
            Some(slot_chunks) => Ok(slot_chunks.clone()),
            _ => Err(CollectError::CollectError("must specify slots".to_string())),
        }
    }

    pub(crate) fn call_data_chunks(&self) -> Result<Vec<CallDataChunk>, CollectError> {
        match &self.call_data_chunks {
            Some(call_data_chunks) => Ok(call_data_chunks.clone()),
            _ => Err(CollectError::CollectError("must specify call_data".to_string())),
        }
    }

    /// apply arg aliases
    pub fn apply_arg_aliases(&self, arg_aliases: HashMap<String, String>) -> RowFilter {
        let mut row_filter: RowFilter = self.clone();
        for (from, to) in arg_aliases.iter() {
            row_filter = match from.as_str() {
                "address" => match to.as_str() {
                    "contract" => RowFilter {
                        contract_chunks: row_filter.address_chunks.clone(),
                        address_chunks: None,
                        ..row_filter.clone()
                    },
                    "to_address" => RowFilter {
                        to_address_chunks: row_filter.address_chunks.clone(),
                        address_chunks: None,
                        ..row_filter.clone()
                    },
                    _ => {
                        panic!("invalid alias")
                    }
                },
                "contract" => match to.as_str() {
                    "address" => RowFilter {
                        address_chunks: row_filter.address_chunks.clone(),
                        contract_chunks: None,
                        ..row_filter.clone()
                    },
                    "to_address" => RowFilter {
                        to_address_chunks: row_filter.contract_chunks.clone(),
                        contract_chunks: None,
                        ..row_filter.clone()
                    },
                    _ => {
                        panic!("invalid alias")
                    }
                },
                "to_address" => match to.as_str() {
                    "address" => RowFilter {
                        address_chunks: row_filter.to_address_chunks.clone(),
                        to_address_chunks: None,
                        ..row_filter.clone()
                    },
                    "contract" => RowFilter {
                        contract_chunks: row_filter.to_address_chunks.clone(),
                        to_address_chunks: None,
                        ..row_filter.clone()
                    },
                    _ => {
                        panic!("invalid alias")
                    }
                },
                _ => {
                    panic!("invalid alias")
                }
            };
        }
        row_filter
    }
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
