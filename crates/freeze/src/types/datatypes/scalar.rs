use crate::datasets::*;
use crate::define_datatypes;
use crate::types::columns::ColumnData;
use crate::ColumnType;
use crate::*;
use polars::prelude::*;
use std::collections::HashMap;

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

impl Datatype {
    fn alias_map() -> HashMap<String, Datatype> {
        let mut map = HashMap::new();
        for datatype in Datatype::all() {
            let key = datatype.name();
            if map.contains_key(&key) {
                panic!("conflict in datatype names")
            }
            map.insert(key, datatype);
            for key in datatype.aliases().into_iter() {
                if map.contains_key(key) {
                    panic!("conflict in datatype names")
                }
                map.insert(key.to_owned(), datatype);
            }
        }
        map
    }
}

impl std::str::FromStr for Datatype {
    type Err = ParseError;

    fn from_str(s: &str) -> Result<Datatype, ParseError> {
        let mut map = Datatype::alias_map();
        map.remove(s)
            .ok_or_else(|| ParseError::ParseError(format!("no datatype matches input: {}", s)))
    }
}

/// Dataset manages collection and management of a particular datatype
pub trait Dataset: Sync + Send {
    /// name of Dataset
    fn name() -> &'static str;

    /// alias of Dataset
    fn aliases() -> Vec<&'static str> {
        vec![]
    }

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
