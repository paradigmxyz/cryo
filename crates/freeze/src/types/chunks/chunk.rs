use crate::types::{Datatype, FileError, FileOutput};
use std::{collections::HashMap, path::PathBuf};

use super::{binary_chunk::BinaryChunk, chunk_ops::ChunkData, number_chunk::NumberChunk};

/// block chunk
pub type BlockChunk = NumberChunk;

/// transaction chunk
pub type TransactionChunk = BinaryChunk;

/// address chunk
pub type AddressChunk = BinaryChunk;

/// slot chunk
pub type SlotChunk = BinaryChunk;

/// call data chunk
pub type CallDataChunk = BinaryChunk;

/// topic chunk
pub type TopicChunk = BinaryChunk;

/// Chunk of data
#[derive(Debug, Clone)]
pub enum Chunk {
    /// block chunk
    Block(BlockChunk),

    /// transaction chunk
    Transaction(TransactionChunk),

    /// address chunk chunk
    Address(AddressChunk),
}

/// Chunk methods
impl Chunk {
    /// get filepath for chunk
    pub fn filepath(
        &self,
        datatype: &Datatype,
        file_output: &FileOutput,
        chunk_label: &Option<String>,
    ) -> Result<PathBuf, FileError> {
        match self {
            Chunk::Block(chunk) => chunk.filepath(datatype, file_output, chunk_label),
            Chunk::Transaction(chunk) => chunk.filepath(datatype, file_output, chunk_label),
            Chunk::Address(chunk) => chunk.filepath(datatype, file_output, chunk_label),
        }
    }

    /// get filepath for chunk
    pub fn filepaths(
        &self,
        datatypes: Vec<&Datatype>,
        file_output: &FileOutput,
        chunk_label: &Option<String>,
    ) -> Result<HashMap<Datatype, PathBuf>, FileError> {
        let mut paths = HashMap::new();
        for datatype in datatypes {
            let path = self.filepath(datatype, file_output, chunk_label)?;
            paths.insert(*datatype, path);
        }
        Ok(paths)
    }
}
