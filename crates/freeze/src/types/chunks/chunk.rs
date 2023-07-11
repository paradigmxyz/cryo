use crate::types::FileError;
use crate::types::FreezeOpts;

use super::binary_chunk::BinaryChunk;
use super::chunk_ops::ChunkData;
use super::number_chunk::NumberChunk;

/// block chunk
pub type BlockChunk = NumberChunk;

/// transaction chunk
pub type TransactionChunk = BinaryChunk;

/// address chunk
pub type AddressChunk = BinaryChunk;

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
    pub fn filepath(&self, name: &str, opts: &FreezeOpts) -> Result<String, FileError> {
        match self {
            Chunk::Block(chunk) => chunk.filepath(name, opts),
            Chunk::Transaction(chunk) => chunk.filepath(name, opts),
            Chunk::Address(chunk) => chunk.filepath(name, opts),
        }
    }
}
