use std::collections::HashMap;

use polars::prelude::*;
use async_trait;

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

#[async_trait::async_trait]
pub trait Dataset: Sync + Send {
    fn name(&self) -> &'static str;
    fn column_types(&self) -> HashMap<&'static str, ColumnType>;
    fn default_columns(&self) -> Vec<&'static str>;
    fn default_sort(&self) -> Vec<String>;
    async fn collect_dataset(&self, _block_chunk: &BlockChunk, _opts: &FreezeOpts) -> DataFrame;
    // async fn collect_datasets(
    //     &self,
    //     block_chunk: &BlockChunk,
    //     extra_datsets: &Vec<Box<dyn Dataset>>,
    //     opts: &FreezeOpts,
    // ) -> Vec<&mut DataFrame> {
    //     if extra_datsets.len() > 0 {
    //         panic!("custom collect_datasets() required when using extra_datasets")
    //     }
    //     vec![self.collect_dataset(block_chunk, opts).await]
    // }
}

