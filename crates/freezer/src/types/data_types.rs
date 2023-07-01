use std::collections::HashMap;
// use std::collections::HashSet;

use async_trait;
use polars::prelude::*;

use crate::types::BlockChunk;
use crate::types::ColumnType;
use crate::types::FreezeOpts;
use crate::types::error_types;

pub struct BalanceDiffs;
pub struct Blocks;
pub struct CodeDiffs;
pub struct Logs;
pub struct NonceDiffs;
pub struct StorageDiffs;
pub struct Traces;
pub struct Transactions;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Datatype {
    BalanceDiffs,
    Blocks,
    CodeDiffs,
    Logs,
    NonceDiffs,
    Transactions,
    Traces,
    StorageDiffs,
}

impl Datatype {
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
        }
    }
}

#[async_trait::async_trait]
pub trait Dataset: Sync + Send {
    fn datatype(&self) -> Datatype;
    fn name(&self) -> &'static str;
    fn column_types(&self) -> HashMap<&'static str, ColumnType>;
    fn default_columns(&self) -> Vec<&'static str>;
    fn default_sort(&self) -> Vec<String>;
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
