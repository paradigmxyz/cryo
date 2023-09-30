use crate::types::{Dataset, Datatype};
use async_trait;
use std::collections::{HashMap, HashSet};

/// Blocks and Transactions datasets
pub struct BlocksAndTransactions;
/// State Diff datasets
pub struct StateDiffs;

/// enum of possible sets of datatypes that cryo can collect
/// used when multiple datatypes are collected together
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, serde::Serialize)]
pub enum MultiDatatype {
    /// blocks and transactions
    BlocksAndTransactions,

    /// balance diffs, code diffs, nonce diffs, and storage diffs
    StateDiffs,
}

impl MultiDatatype {
    /// individual datatypes
    pub fn datatypes(&self) -> Vec<Datatype> {
        match &self {
            MultiDatatype::BlocksAndTransactions => vec![Datatype::Blocks, Datatype::Transactions],
            MultiDatatype::StateDiffs => vec![
                Datatype::BalanceDiffs,
                Datatype::CodeDiffs,
                Datatype::NonceDiffs,
                Datatype::StorageDiffs,
            ],
        }
    }

    /// return all variants of multi datatype
    pub fn variants() -> Vec<MultiDatatype> {
        vec![MultiDatatype::BlocksAndTransactions, MultiDatatype::StateDiffs]
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
        self.datatypes().iter().map(|dt| (*dt, dt.dataset())).collect()
    }
}
