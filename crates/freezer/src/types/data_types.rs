use std::collections::HashMap;
// use std::collections::HashSet;

use async_trait;
use ethers::prelude::*;
use polars::prelude::*;
use thiserror::Error;

use crate::types::BlockChunk;
use crate::types::ColumnType;
use crate::types::FreezeOpts;

pub struct Blocks;
pub struct Logs;
pub struct Transactions;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Datatype {
    Blocks,
    Logs,
    Transactions,
}

impl Datatype {
    pub fn dataset(&self) -> Box<dyn Dataset> {
        match *self {
            Datatype::Blocks => Box::new(Blocks),
            Datatype::Logs => Box::new(Logs),
            Datatype::Transactions => Box::new(Transactions),
        }
    }
}

#[derive(Error, Debug)]
pub enum CollectError {
    #[error("Failed to get block: {0}")]
    ProviderError(#[source] ProviderError), // Replace ProviderErrorType with the actual error type from the provider

    #[error("Task failed: {0}")]
    TaskFailed(#[source] tokio::task::JoinError),

    #[error("Failed to convert to DataFrme: {0}")]
    PolarsError(#[from] PolarsError),
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
    ) -> Result<DataFrame, CollectError>;

    // async fn collect_chunk_with_extras(
    //     &self,
    //     block_chunk: &BlockChunk,
    //     extras: &HashSet<Datatype>,
    //     opts: &FreezeOpts,
    // ) -> HashMap<Datatype, DataFrame> {
    //     if !extras.is_empty() {
    //         panic!("custom collect_datasets() required when using extras");
    //     }
    //     let df = self.collect_chunk(block_chunk, opts).await;
    //     [(self.datatype(), df)].iter().cloned().collect()
    // }
}
