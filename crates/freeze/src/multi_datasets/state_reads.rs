use crate::*;
use alloy::{primitives::Address, rpc::types::trace::geth::AccountState};
use polars::prelude::*;
use std::collections::{BTreeMap, HashMap};

/// StateReads
#[derive(Default)]
pub struct StateReads(
    balance_reads::BalanceReads,
    code_reads::CodeReads,
    nonce_reads::NonceReads,
    storage_reads::StorageReads,
);

type BlockTxsTraces = (Option<u32>, Vec<Option<RawBytes>>, Vec<BTreeMap<Address, AccountState>>);

impl ToDataFrames for StateReads {
    fn create_dfs(
        self,
        schemas: &HashMap<Datatype, Table>,
        chain_id: u64,
    ) -> R<HashMap<Datatype, DataFrame>> {
        let StateReads(balances, codes, nonces, storages) = self;
        let mut output = HashMap::new();
        if schemas.get(&Datatype::BalanceReads).is_some() {
            output.extend(balances.create_dfs(schemas, chain_id)?);
        }
        if schemas.get(&Datatype::CodeReads).is_some() {
            output.extend(codes.create_dfs(schemas, chain_id)?);
        }
        if schemas.get(&Datatype::NonceReads).is_some() {
            output.extend(nonces.create_dfs(schemas, chain_id)?);
        }
        if schemas.get(&Datatype::StorageReads).is_some() {
            output.extend(storages.create_dfs(schemas, chain_id)?);
        }
        Ok(output)
    }
}

#[async_trait::async_trait]
impl CollectByBlock for StateReads {
    type Response = BlockTxsTraces;

    async fn extract(request: Params, source: Arc<Source>, query: Arc<Query>) -> R<Self::Response> {
        let include_txs = match (
            query.schemas.get(&Datatype::BalanceReads),
            query.schemas.get(&Datatype::CodeReads),
            query.schemas.get(&Datatype::NonceReads),
            query.schemas.get(&Datatype::StorageReads),
        ) {
            (Some(schema), _, _, _) |
            (_, Some(schema), _, _) |
            (_, _, Some(schema), _) |
            (_, _, _, Some(schema)) => schema.has_column("transaction_hash"),
            _ => false,
        };
        source.geth_debug_trace_block_prestate(request.block_number()? as u32, include_txs).await
    }

    fn transform(response: Self::Response, columns: &mut Self, query: &Arc<Query>) -> R<()> {
        process_state_reads(response, columns, &query.schemas)
    }
}

#[async_trait::async_trait]
impl CollectByTransaction for StateReads {
    type Response = BlockTxsTraces;

    async fn extract(request: Params, source: Arc<Source>, query: Arc<Query>) -> R<Self::Response> {
        let include_block_number = match (
            query.schemas.get(&Datatype::BalanceReads),
            query.schemas.get(&Datatype::CodeReads),
            query.schemas.get(&Datatype::NonceReads),
            query.schemas.get(&Datatype::StorageReads),
        ) {
            (Some(schema), _, _, _) |
            (_, Some(schema), _, _) |
            (_, _, Some(schema), _) |
            (_, _, _, Some(schema)) => schema.has_column("block_number"),
            _ => false,
        };
        let tx = request.transaction_hash()?;
        source.geth_debug_trace_transaction_prestate(tx, include_block_number).await
    }

    fn transform(response: Self::Response, columns: &mut Self, query: &Arc<Query>) -> R<()> {
        process_state_reads(response, columns, &query.schemas)
    }
}

fn process_state_reads(
    response: BlockTxsTraces,
    columns: &mut StateReads,
    schemas: &HashMap<Datatype, Table>,
) -> R<()> {
    let StateReads(balances, codes, nonces, storages) = columns;
    if schemas.get(&Datatype::BalanceReads).is_some() {
        balance_reads::process_balance_reads(&response, balances, schemas)?;
    }
    if schemas.get(&Datatype::CodeReads).is_some() {
        code_reads::process_code_reads(&response, codes, schemas)?;
    }
    if schemas.get(&Datatype::NonceReads).is_some() {
        nonce_reads::process_nonce_reads(&response, nonces, schemas)?;
    }
    if schemas.get(&Datatype::StorageReads).is_some() {
        storage_reads::process_storage_reads(&response, storages, schemas)?;
    }
    Ok(())
}
