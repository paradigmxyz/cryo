use crate::ColumnType;
use std::collections::HashMap;
use crate::types::columns::ColumnData;
use crate::datasets::*;

macro_rules! define_datatypes {
    ($($datatype:ident),* $(,)?) => {
        /// Datatypes
        #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, serde::Serialize)]
        pub enum Datatype {
            $(
                /// $datatype
                $datatype,
            )*
        }

        impl Datatype {
            /// name of datatype
            pub fn name(&self) -> &'static str {
                match *self {
                    $(Datatype::$datatype => stringify!($datatype),)*
                }
            }

            /// default sorting columns of datatype
            pub fn default_sort(&self) -> Vec<String> {
                match *self {
                    $(Datatype::$datatype => $datatype::default_sort(),)*
                }
            }

            /// default columns of datatype
            pub fn default_columns(&self) -> Vec<&'static str> {
                match *self {
                    $(Datatype::$datatype => $datatype::base_default_columns(),)*
                }
            }

            /// default blocks of datatype
            pub fn default_blocks(&self) -> Option<String> {
                match *self {
                    $(Datatype::$datatype => $datatype::base_default_blocks(),)*
                }
            }

            /// default column types of datatype
            pub fn column_types(&self) -> HashMap<&'static str, ColumnType> {
                match *self {
                    $(Datatype::$datatype => $datatype::column_types(),)*
                }
            }

            /// aliases of datatype
            pub fn arg_aliases(&self) -> HashMap<String, String> {
                match *self {
                    $(Datatype::$datatype => $datatype::base_arg_aliases(),)*
                }
            }
        }
    };
}

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
