use crate::ColumnType;
use std::collections::HashMap;
use crate::types::columns::ColumnData;
use crate::datasets::*;

/// enum of possible datatypes that cryo can collect
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, serde::Serialize)]
pub enum Datatype {
    /// Balance Diffs
    BalanceDiffs,
    /// Balances
    Balances,
    /// Blocks
    Blocks,
    /// Code Diffs
    CodeDiffs,
    /// Codes
    Codes,
    /// Contracts
    Contracts,
    /// Erc20 Balances
    Erc20Balances,
    /// Erc20 Metadata
    Erc20Metadata,
    /// Erc20 Supplies
    Erc20Supplies,
    /// Erc20 Transfers
    Erc20Transfers,
    /// Erc721 Metadata
    Erc721Metadata,
    /// Erc721 Transfers
    Erc721Transfers,
    /// Eth Calls
    EthCalls,
    /// Logs
    Logs,
    /// Nonce Diffs
    NonceDiffs,
    /// Nonces
    Nonces,
    /// Storage Diffs
    StorageDiffs,
    /// Storage
    Storages,
    /// Traces
    Traces,
    /// Trace Calls
    TraceCalls,
    /// Transactions
    Transactions,
    /// Transaction Addresses
    TransactionAddresses,
    /// VmTraces
    VmTraces,
    /// Native Transfers
    NativeTransfers,
}


impl Datatype {
    /// get the Dataset struct corresponding to Datatype
    pub fn name(&self) -> &'static str {
        match *self {
            Datatype::BalanceDiffs => BalanceDiffs::name(),
            Datatype::Balances => Balances::name(),
            Datatype::Blocks => Blocks::name(),
            Datatype::CodeDiffs => CodeDiffs::name(),
            Datatype::Codes => Codes::name(),
            Datatype::Contracts => Contracts::name(),
            Datatype::Erc20Balances => Erc20Balances::name(),
            Datatype::Erc20Metadata => Erc20Metadata::name(),
            Datatype::Erc20Supplies => Erc20Supplies::name(),
            Datatype::Erc20Transfers => Erc20Transfers::name(),
            Datatype::Erc721Metadata => Erc721Metadata::name(),
            Datatype::Erc721Transfers => Erc721Transfers::name(),
            Datatype::EthCalls => EthCalls::name(),
            Datatype::Logs => Logs::name(),
            Datatype::NativeTransfers => NativeTransfers::name(),
            Datatype::NonceDiffs => NonceDiffs::name(),
            Datatype::Nonces => Nonces::name(),
            Datatype::StorageDiffs => StorageDiffs::name(),
            Datatype::Storages => Storages::name(),
            Datatype::Traces => Traces::name(),
            Datatype::TraceCalls => TraceCalls::name(),
            Datatype::Transactions => Transactions::name(),
            Datatype::TransactionAddresses => TransactionAddresses::name(),
            Datatype::VmTraces => VmTraces::name(),
        }
    }

    /// get the Dataset struct corresponding to Datatype
    pub fn default_sort(&self) -> Vec<String> {
        match *self {
            Datatype::BalanceDiffs => BalanceDiffs::default_sort(),
            Datatype::Balances => Balances::default_sort(),
            Datatype::Blocks => Blocks::default_sort(),
            Datatype::CodeDiffs => CodeDiffs::default_sort(),
            Datatype::Codes => Codes::default_sort(),
            Datatype::Contracts => Contracts::default_sort(),
            Datatype::Erc20Balances => Erc20Balances::default_sort(),
            Datatype::Erc20Metadata => Erc20Metadata::default_sort(),
            Datatype::Erc20Supplies => Erc20Supplies::default_sort(),
            Datatype::Erc20Transfers => Erc20Transfers::default_sort(),
            Datatype::Erc721Metadata => Erc721Metadata::default_sort(),
            Datatype::Erc721Transfers => Erc721Transfers::default_sort(),
            Datatype::EthCalls => EthCalls::default_sort(),
            Datatype::Logs => Logs::default_sort(),
            Datatype::NativeTransfers => NativeTransfers::default_sort(),
            Datatype::NonceDiffs => NonceDiffs::default_sort(),
            Datatype::Nonces => Nonces::default_sort(),
            Datatype::StorageDiffs => StorageDiffs::default_sort(),
            Datatype::Storages => Storages::default_sort(),
            Datatype::Traces => Traces::default_sort(),
            Datatype::TraceCalls => TraceCalls::default_sort(),
            Datatype::Transactions => Transactions::default_sort(),
            Datatype::TransactionAddresses => TransactionAddresses::default_sort(),
            Datatype::VmTraces => VmTraces::default_sort(),
        }
    }

    /// get the Dataset struct corresponding to Datatype
    pub fn default_columns(&self) -> Vec<&'static str> {
        match *self {
            Datatype::BalanceDiffs => BalanceDiffs::base_default_columns(),
            Datatype::Balances => Balances::base_default_columns(),
            Datatype::Blocks => Blocks::base_default_columns(),
            Datatype::CodeDiffs => CodeDiffs::base_default_columns(),
            Datatype::Codes => Codes::base_default_columns(),
            Datatype::Contracts => Contracts::base_default_columns(),
            Datatype::Erc20Balances => Erc20Balances::base_default_columns(),
            Datatype::Erc20Metadata => Erc20Metadata::base_default_columns(),
            Datatype::Erc20Supplies => Erc20Supplies::base_default_columns(),
            Datatype::Erc20Transfers => Erc20Transfers::base_default_columns(),
            Datatype::Erc721Metadata => Erc721Metadata::base_default_columns(),
            Datatype::Erc721Transfers => Erc721Transfers::base_default_columns(),
            Datatype::EthCalls => EthCalls::base_default_columns(),
            Datatype::Logs => Logs::base_default_columns(),
            Datatype::NativeTransfers => NativeTransfers::base_default_columns(),
            Datatype::NonceDiffs => NonceDiffs::base_default_columns(),
            Datatype::Nonces => Nonces::base_default_columns(),
            Datatype::StorageDiffs => StorageDiffs::base_default_columns(),
            Datatype::Storages => Storages::base_default_columns(),
            Datatype::Traces => Traces::base_default_columns(),
            Datatype::TraceCalls => TraceCalls::base_default_columns(),
            Datatype::Transactions => Transactions::base_default_columns(),
            Datatype::TransactionAddresses => TransactionAddresses::base_default_columns(),
            Datatype::VmTraces => VmTraces::base_default_columns(),
        }
    }

    /// get the Dataset struct corresponding to Datatype
    pub fn default_blocks(&self) -> Option<String> {
        match *self {
            Datatype::BalanceDiffs => BalanceDiffs::base_default_blocks(),
            Datatype::Balances => Balances::base_default_blocks(),
            Datatype::Blocks => Blocks::base_default_blocks(),
            Datatype::CodeDiffs => CodeDiffs::base_default_blocks(),
            Datatype::Codes => Codes::base_default_blocks(),
            Datatype::Contracts => Contracts::base_default_blocks(),
            Datatype::Erc20Balances => Erc20Balances::base_default_blocks(),
            Datatype::Erc20Metadata => Erc20Metadata::base_default_blocks(),
            Datatype::Erc20Supplies => Erc20Supplies::base_default_blocks(),
            Datatype::Erc20Transfers => Erc20Transfers::base_default_blocks(),
            Datatype::Erc721Metadata => Erc721Metadata::base_default_blocks(),
            Datatype::Erc721Transfers => Erc721Transfers::base_default_blocks(),
            Datatype::EthCalls => EthCalls::base_default_blocks(),
            Datatype::Logs => Logs::base_default_blocks(),
            Datatype::NativeTransfers => NativeTransfers::base_default_blocks(),
            Datatype::NonceDiffs => NonceDiffs::base_default_blocks(),
            Datatype::Nonces => Nonces::base_default_blocks(),
            Datatype::StorageDiffs => StorageDiffs::base_default_blocks(),
            Datatype::Storages => Storages::base_default_blocks(),
            Datatype::Traces => Traces::base_default_blocks(),
            Datatype::TraceCalls => TraceCalls::base_default_blocks(),
            Datatype::Transactions => Transactions::base_default_blocks(),
            Datatype::TransactionAddresses => TransactionAddresses::base_default_blocks(),
            Datatype::VmTraces => VmTraces::base_default_blocks(),
        }
    }


    /// get the Dataset struct corresponding to Datatype
    pub fn column_types(&self) -> HashMap<&'static str, ColumnType> {
        match *self {
            Datatype::BalanceDiffs => BalanceDiffs::column_types(),
            Datatype::Balances => Balances::column_types(),
            Datatype::Blocks => Blocks::column_types(),
            Datatype::CodeDiffs => CodeDiffs::column_types(),
            Datatype::Codes => Codes::column_types(),
            Datatype::Contracts => Contracts::column_types(),
            Datatype::Erc20Balances => Erc20Balances::column_types(),
            Datatype::Erc20Metadata => Erc20Metadata::column_types(),
            Datatype::Erc20Supplies => Erc20Supplies::column_types(),
            Datatype::Erc20Transfers => Erc20Transfers::column_types(),
            Datatype::Erc721Metadata => Erc721Metadata::column_types(),
            Datatype::Erc721Transfers => Erc721Transfers::column_types(),
            Datatype::EthCalls => EthCalls::column_types(),
            Datatype::Logs => Logs::column_types(),
            Datatype::NativeTransfers => NativeTransfers::column_types(),
            Datatype::NonceDiffs => NonceDiffs::column_types(),
            Datatype::Nonces => Nonces::column_types(),
            Datatype::StorageDiffs => StorageDiffs::column_types(),
            Datatype::Storages => Storages::column_types(),
            Datatype::Traces => Traces::column_types(),
            Datatype::TraceCalls => TraceCalls::column_types(),
            Datatype::Transactions => Transactions::column_types(),
            Datatype::TransactionAddresses => TransactionAddresses::column_types(),
            Datatype::VmTraces => VmTraces::column_types(),
        }
    }

    /// get the Dataset struct corresponding to Datatype
    pub fn arg_aliases(&self) -> HashMap<String, String> {
        match *self {
            Datatype::BalanceDiffs => BalanceDiffs::base_arg_aliases(),
            Datatype::Balances => Balances::base_arg_aliases(),
            Datatype::Blocks => Blocks::base_arg_aliases(),
            Datatype::CodeDiffs => CodeDiffs::base_arg_aliases(),
            Datatype::Codes => Codes::base_arg_aliases(),
            Datatype::Contracts => Contracts::base_arg_aliases(),
            Datatype::Erc20Balances => Erc20Balances::base_arg_aliases(),
            Datatype::Erc20Metadata => Erc20Metadata::base_arg_aliases(),
            Datatype::Erc20Supplies => Erc20Supplies::base_arg_aliases(),
            Datatype::Erc20Transfers => Erc20Transfers::base_arg_aliases(),
            Datatype::Erc721Metadata => Erc721Metadata::base_arg_aliases(),
            Datatype::Erc721Transfers => Erc721Transfers::base_arg_aliases(),
            Datatype::EthCalls => EthCalls::base_arg_aliases(),
            Datatype::Logs => Logs::base_arg_aliases(),
            Datatype::NativeTransfers => NativeTransfers::base_arg_aliases(),
            Datatype::NonceDiffs => NonceDiffs::base_arg_aliases(),
            Datatype::Nonces => Nonces::base_arg_aliases(),
            Datatype::StorageDiffs => StorageDiffs::base_arg_aliases(),
            Datatype::Storages => Storages::base_arg_aliases(),
            Datatype::Traces => Traces::base_arg_aliases(),
            Datatype::TraceCalls => TraceCalls::base_arg_aliases(),
            Datatype::Transactions => Transactions::base_arg_aliases(),
            Datatype::TransactionAddresses => TransactionAddresses::base_arg_aliases(),
            Datatype::VmTraces => VmTraces::base_arg_aliases(),
        }
    }
}

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
