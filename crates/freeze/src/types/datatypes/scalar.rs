use crate::datasets::*;
use crate::define_datatypes;
use crate::types::columns::ColumnData;
use crate::ColumnType;
use std::collections::HashMap;
use crate::*;
use polars::prelude::*;

define_datatypes!(
    BalanceDiffs,
    Balances,
    Blocks,
    CodeDiffs,
    Codes,
    Contracts,
    Erc20Balances,
    Erc20Metadata,
    Erc20Supplies,
    Erc20Transfers,
    Erc721Metadata,
    Erc721Transfers,
    EthCalls,
    Logs,
    NonceDiffs,
    Nonces,
    StorageDiffs,
    Storages,
    Traces,
    TraceCalls,
    Transactions,
    TransactionAddresses,
    VmTraces,
    NativeTransfers,
);

/// Dataset manages collection and management of a particular datatype
pub trait Dataset: Sync + Send {
    /// name of Dataset
    fn name() -> &'static str;

    /// default sort order for dataset
    fn default_sort() -> Vec<String>;

    /// default columns extracted for Dataset
    fn default_columns() -> Option<Vec<&'static str>> {
        None
    }

    /// default blocks for dataset
    fn default_blocks() -> Option<String> {
        None
    }

    /// input arg aliases
    fn arg_aliases() -> Option<HashMap<String, String>> {
        None
    }
}

