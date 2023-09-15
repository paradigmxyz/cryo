use std::collections::HashMap;

use async_trait;
use polars::prelude::*;

use crate::types::{
    AddressChunk, BlockChunk, Chunk, CollectError, ColumnType, RowFilter, Source, Table,
    TransactionChunk,
};

/// Balance Diffs Dataset
pub struct BalanceDiffs;
/// Balances Dataset
pub struct Balances;
/// Blocks Dataset
pub struct Blocks;
/// Code Diffs Dataset
pub struct CodeDiffs;
/// Codes Dataset
pub struct Codes;
/// Contracts Dataset
pub struct Contracts;
/// Erc20 Balances Dataset
pub struct Erc20Balances;
/// Erc20 Metadata Dataset
pub struct Erc20Metadata;
/// Erc20 Supplies Dataset
pub struct Erc20Supplies;
/// Erc20 Transfers Dataset
pub struct Erc20Transfers;
/// Erc721 Metadata Dataset
pub struct Erc721Metadata;
/// Erc721 Transfers Dataset
pub struct Erc721Transfers;
/// Eth Calls Dataset
pub struct EthCalls;
/// Logs Dataset
pub struct Logs;
/// Nonce Diffs Dataset
pub struct NonceDiffs;
/// Nonces Dataset
pub struct Nonces;
/// Storage Diffs Dataset
pub struct StorageDiffs;
/// Storage Dataset
pub struct Storages;
/// Traces Dataset
pub struct Traces;
/// Trace Calls Dataset
pub struct TraceCalls;
/// Transactions Dataset
pub struct Transactions;
/// Transaction Addresses Dataset
pub struct TransactionAddresses;
/// VmTraces Dataset
pub struct VmTraces;
/// Native Transfers Dataset
pub struct NativeTransfers;

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
    pub fn dataset(&self) -> Box<dyn Dataset> {
        match *self {
            Datatype::BalanceDiffs => Box::new(BalanceDiffs),
            Datatype::Balances => Box::new(Balances),
            Datatype::Blocks => Box::new(Blocks),
            Datatype::CodeDiffs => Box::new(CodeDiffs),
            Datatype::Codes => Box::new(Codes),
            Datatype::Contracts => Box::new(Contracts),
            Datatype::Erc20Balances => Box::new(Erc20Balances),
            Datatype::Erc20Metadata => Box::new(Erc20Metadata),
            Datatype::Erc20Supplies => Box::new(Erc20Supplies),
            Datatype::Erc20Transfers => Box::new(Erc20Transfers),
            Datatype::Erc721Metadata => Box::new(Erc721Metadata),
            Datatype::Erc721Transfers => Box::new(Erc721Transfers),
            Datatype::EthCalls => Box::new(EthCalls),
            Datatype::Logs => Box::new(Logs),
            Datatype::NonceDiffs => Box::new(NonceDiffs),
            Datatype::Nonces => Box::new(Nonces),
            Datatype::StorageDiffs => Box::new(StorageDiffs),
            Datatype::Storages => Box::new(Storages),
            Datatype::Traces => Box::new(Traces),
            Datatype::TraceCalls => Box::new(TraceCalls),
            Datatype::Transactions => Box::new(Transactions),
            Datatype::TransactionAddresses => Box::new(TransactionAddresses),
            Datatype::VmTraces => Box::new(VmTraces),
            Datatype::NativeTransfers => Box::new(NativeTransfers),
        }
    }
}

/// Dataset manages collection and management of a particular datatype
#[async_trait::async_trait]
pub trait Dataset: Sync + Send {
    // type CollectOpts;

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

    /// default blocks for dataset
    fn default_blocks(&self) -> Option<String> {
        None
    }

    /// input arg aliases
    fn arg_aliases(&self) -> HashMap<String, String> {
        HashMap::new()
    }

    /// collect dataset for a particular chunk
    async fn collect_chunk(
        &self,
        chunk: &Chunk,
        source: &Source,
        schema: &Table,
        filter: Option<&RowFilter>,
    ) -> Result<DataFrame, CollectError> {
        match chunk {
            Chunk::Block(chunk) => self.collect_block_chunk(chunk, source, schema, filter).await,
            Chunk::Transaction(chunk) => {
                self.collect_transaction_chunk(chunk, source, schema, filter).await
            }
            Chunk::Address(chunk) => {
                self.collect_address_chunk(chunk, source, schema, filter).await
            }
        }
    }

    /// collect dataset for a particular block chunk
    async fn collect_block_chunk(
        &self,
        _chunk: &BlockChunk,
        _source: &Source,
        _schema: &Table,
        _filter: Option<&RowFilter>,
    ) -> Result<DataFrame, CollectError> {
        panic!("block_chunk collection not implemented for {}", self.name())
    }

    /// collect dataset for a particular transaction chunk
    async fn collect_transaction_chunk(
        &self,
        _chunk: &TransactionChunk,
        _source: &Source,
        _schema: &Table,
        _filter: Option<&RowFilter>,
    ) -> Result<DataFrame, CollectError> {
        panic!("transaction_chunk collection not implemented for {}", self.name())
    }

    /// collect dataset for a particular transaction chunk
    async fn collect_address_chunk(
        &self,
        _chunk: &AddressChunk,
        _source: &Source,
        _schema: &Table,
        _filter: Option<&RowFilter>,
    ) -> Result<DataFrame, CollectError> {
        panic!("transaction_chunk collection not implemented for {}", self.name())
    }
}
