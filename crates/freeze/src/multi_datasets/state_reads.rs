use crate::*;
use ethers::prelude::*;
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

type BlockTxsTraces = (Option<u32>, Vec<Option<Vec<u8>>>, Vec<BTreeMap<H160, AccountState>>);

impl ToDataFrames for StateReads {
    fn create_dfs(
        self,
        schemas: &HashMap<Datatype, Table>,
        chain_id: u64,
    ) -> R<HashMap<Datatype, DataFrame>> {
        let StateReads(balances, codes, nonces, storages) = self;
        let mut output = HashMap::new();
        output.extend(balances.create_dfs(schemas, chain_id)?);
        output.extend(codes.create_dfs(schemas, chain_id)?);
        output.extend(nonces.create_dfs(schemas, chain_id)?);
        output.extend(storages.create_dfs(schemas, chain_id)?);
        Ok(output)
    }
}

#[async_trait::async_trait]
impl CollectByBlock for StateReads {
    type Response = BlockTxsTraces;

    async fn extract(request: Params, source: Arc<Source>, query: Arc<Query>) -> R<Self::Response> {
        let schema =
            query.schemas.get(&Datatype::StorageReads).ok_or(err("schema not provided"))?;
        let include_txs = schema.has_column("transaction_hash");
        source
            .fetcher
            .geth_debug_trace_block_prestate(request.block_number()? as u32, include_txs)
            .await
    }

    fn transform(response: Self::Response, columns: &mut Self, query: &Arc<Query>) -> R<()> {
        process_state_reads(response, columns, &query.schemas)
    }
}

#[async_trait::async_trait]
impl CollectByTransaction for StateReads {
    type Response = BlockTxsTraces;

    async fn extract(request: Params, source: Arc<Source>, query: Arc<Query>) -> R<Self::Response> {
        let schema =
            query.schemas.get(&Datatype::StorageReads).ok_or(err("schema not provided"))?;
        let include_block_number = schema.has_column("block_number");
        let tx = request.transaction_hash()?;
        source.fetcher.geth_debug_trace_transaction_prestate(tx, include_block_number).await
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
    balance_reads::process_balance_reads(&response, balances, schemas)?;
    code_reads::process_code_reads(&response, codes, schemas)?;
    nonce_reads::process_nonce_reads(&response, nonces, schemas)?;
    storage_reads::process_storage_reads(&response, storages, schemas)?;
    Ok(())
}
