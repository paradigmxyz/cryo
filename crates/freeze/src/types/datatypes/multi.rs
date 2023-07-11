use std::collections::HashMap;
use std::collections::HashSet;

use async_trait;
use polars::prelude::*;

use crate::types::AddressChunk;
use crate::types::BlockChunk;
use crate::types::Chunk;
use crate::types::CollectError;
use crate::types::Dataset;
use crate::types::Datatype;
use crate::types::FreezeOpts;
use crate::types::TransactionChunk;

/// Blocks and Transactions datasets
pub struct BlocksAndTransactions;
/// State Diff datasets
pub struct StateDiffs;

/// enum of possible sets of datatypes that cryo can collect
/// used when multiple datatypes are collected together
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum MultiDatatype {
    /// blocks and transactions
    BlocksAndTransactions,

    /// balance diffs, code diffs, nonce diffs, and storage diffs
    StateDiffs,
}

impl MultiDatatype {
    /// return all variants of multi datatype
    pub fn variants() -> Vec<MultiDatatype> {
        vec![
            MultiDatatype::BlocksAndTransactions,
            MultiDatatype::StateDiffs,
        ]
    }

    /// return MultiDataset corresponding to MultiDatatype
    pub fn multi_dataset(&self) -> Box<dyn MultiDataset> {
        match self {
            MultiDatatype::BlocksAndTransactions => Box::new(BlocksAndTransactions),
            MultiDatatype::StateDiffs => Box::new(StateDiffs),
        }
    }
}

/// MultiDataset manages multiple datasets that get collected together
#[async_trait::async_trait]
pub trait MultiDataset: Sync + Send {
    /// name of Dataset
    fn name(&self) -> &'static str;

    /// return Datatypes associated with MultiDataset
    fn datatypes(&self) -> HashSet<Datatype>;

    /// return Datasets associated with MultiDataset
    fn datasets(&self) -> HashMap<Datatype, Box<dyn Dataset>> {
        self.datatypes()
            .iter()
            .map(|dt| (*dt, dt.dataset()))
            .collect()
    }

    /// collect dataset for a particular chunk
    async fn collect_chunk(
        &self,
        chunk: &Chunk,
        opts: &FreezeOpts,
    ) -> Result<HashMap<Datatype, DataFrame>, CollectError> {
        match chunk {
            Chunk::Block(chunk) => self.collect_block_chunk(chunk, opts).await,
            Chunk::Transaction(chunk) => self.collect_transaction_chunk(chunk, opts).await,
            Chunk::Address(chunk) => self.collect_address_chunk(chunk, opts).await,
        }
    }

    /// collect dataset for a particular block chunk
    async fn collect_block_chunk(
        &self,
        _chunk: &BlockChunk,
        _opts: &FreezeOpts,
    ) -> Result<HashMap<Datatype, DataFrame>, CollectError> {
        panic!("block_chunk collection not implemented for {}", self.name())
    }

    /// collect dataset for a particular transaction chunk
    async fn collect_transaction_chunk(
        &self,
        _chunk: &TransactionChunk,
        _opts: &FreezeOpts,
    ) -> Result<HashMap<Datatype, DataFrame>, CollectError> {
        panic!(
            "transaction_chunk collection not implemented for {}",
            self.name()
        )
    }

    /// collect dataset for a particular transaction chunk
    async fn collect_address_chunk(
        &self,
        _chunk: &AddressChunk,
        _opts: &FreezeOpts,
    ) -> Result<HashMap<Datatype, DataFrame>, CollectError> {
        panic!(
            "transaction_chunk collection not implemented for {}",
            self.name()
        )
    }
}
