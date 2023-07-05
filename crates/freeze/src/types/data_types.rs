/// type specifications for collected datatypes
use std::collections::HashMap;
// use std::collections::HashSet;

use async_trait;
use polars::prelude::*;

use crate::types::error_types;
use crate::types::BlockChunk;
use crate::types::ColumnType;
use crate::types::FreezeOpts;

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
        _block_chunk: &BlockChunk,
        _opts: &FreezeOpts,
    ) -> Result<DataFrame, error_types::CollectError>;

    // async fn collect_chunk_with_extras(
    //     &self,
    //     block_chunk: &BlockChunk,
    //     extras: &HashSet<Datatype>,
    //     opts: &FreezeOpts,
    // ) -> HashMap<Datatype, DataFrame> {
    //     if !extras.is_empty() {
    //         ...
    //     }
    //     let df = self.collect_chunk(block_chunk, opts).await;
    //     [(self.datatype(), df)].iter().cloned().collect()
    // }
}
