/// type specifications for collected datatypes
use std::collections::HashMap;
use std::collections::HashSet;

use async_trait;
use polars::prelude::*;

use crate::types::error_types;
use crate::types::AddressChunk;
use crate::types::BlockChunk;
use crate::types::Chunk;
use crate::types::CollectError;
use crate::types::ColumnType;
use crate::types::FreezeOpts;
use crate::types::TransactionChunk;

/// Balance Diffs Dataset
pub struct BalanceDiffs;
/// Blocks Dataset
pub struct Blocks;
/// Code Diffs Dataset
pub struct CodeDiffs;
/// Logs Dataset
pub struct Logs;
/// Nonce Diffs Dataset
pub struct NonceDiffs;
/// Storage Diffs Dataset
pub struct StorageDiffs;
/// Traces Dataset
pub struct Traces;
/// Transactions Dataset
pub struct Transactions;
/// VmTraces Dataset
pub struct VmTraces;

/// enum of possible datatypes that cryo can collect
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Datatype {
    /// Balance Diffs
    BalanceDiffs,
    /// Blocks
    Blocks,
    /// Code Diffs
    CodeDiffs,
    /// Logs
    Logs,
    /// Nonce Diffs
    NonceDiffs,
    /// Transactions
    Transactions,
    /// Traces
    Traces,
    /// Storage Diffs
    StorageDiffs,
    /// VmTraces
    VmTraces,
}

impl Datatype {
    /// get the Dataset struct corresponding to Datatype
    pub fn dataset(&self) -> Box<dyn Dataset> {
        match *self {
            Datatype::BalanceDiffs => Box::new(BalanceDiffs),
            Datatype::Blocks => Box::new(Blocks),
            Datatype::CodeDiffs => Box::new(CodeDiffs),
            Datatype::Logs => Box::new(Logs),
            Datatype::NonceDiffs => Box::new(NonceDiffs),
            Datatype::Transactions => Box::new(Transactions),
            Datatype::Traces => Box::new(Traces),
            Datatype::StorageDiffs => Box::new(StorageDiffs),
            Datatype::VmTraces => Box::new(VmTraces),
        }
    }
}

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

/// Dataset manages collection and management of a particular datatype
#[async_trait::async_trait]
pub trait Dataset: Sync + Send {
    /// Datatype enum corresponding to Dataset
    fn datatype(&self) -> Datatype;

    /// name of Dataset
    fn name(&self) -> &'static str;

    /// column types of dataset schema
    fn column_types(&self) -> HashMap<&'static str, ColumnType>;

    /// default columns extracted for Dataset
    fn default_columns(&self) -> Vec<&'static str>;

    /// default sort order for dataset
    fn default_sort(&self) -> Vec<String>;

    /// collect dataset for a particular chunk
    async fn collect_chunk(
        &self,
        chunk: &Chunk,
        opts: &FreezeOpts,
    ) -> Result<DataFrame, error_types::CollectError> {
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
    ) -> Result<DataFrame, error_types::CollectError> {
        panic!("block_chunk collection not implemented for {}", self.name())
    }

    /// collect dataset for a particular transaction chunk
    async fn collect_transaction_chunk(
        &self,
        _chunk: &TransactionChunk,
        _opts: &FreezeOpts,
    ) -> Result<DataFrame, error_types::CollectError> {
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
    ) -> Result<DataFrame, error_types::CollectError> {
        panic!(
            "transaction_chunk collection not implemented for {}",
            self.name()
        )
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
