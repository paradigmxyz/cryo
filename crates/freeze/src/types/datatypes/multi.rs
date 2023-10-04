use crate::types::Datatype;
use async_trait;
use std::collections::HashSet;

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
}

/// MultiDataset manages multiple datasets that get collected together
#[async_trait::async_trait]
pub trait MultiDataset: Sync + Send {
    /// name of Dataset
    fn name(&self) -> &'static str;

    /// return Datatypes associated with MultiDataset
    fn datatypes(&self) -> HashSet<Datatype>;
}
