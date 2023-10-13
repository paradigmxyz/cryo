use crate::*;
use polars::prelude::*;
use std::collections::HashMap;

/// StateDiffs
#[derive(Default)]
pub struct StateDiffs(
    balance_diffs::BalanceDiffs,
    code_diffs::CodeDiffs,
    nonce_diffs::NonceDiffs,
    storage_diffs::StorageDiffs,
);

type BlockTxsTraces = (Option<u32>, Vec<Option<Vec<u8>>>, Vec<ethers::types::BlockTrace>);
type Result<T> = ::core::result::Result<T, CollectError>;

impl ToDataFrames for StateDiffs {
    fn create_dfs(
        self,
        schemas: &HashMap<Datatype, Table>,
        chain_id: u64,
    ) -> Result<HashMap<Datatype, DataFrame>> {
        let StateDiffs(balances, codes, nonces, storages) = self;
        let mut output = HashMap::new();
        output.extend(balances.create_dfs(schemas, chain_id)?);
        output.extend(codes.create_dfs(schemas, chain_id)?);
        output.extend(nonces.create_dfs(schemas, chain_id)?);
        output.extend(storages.create_dfs(schemas, chain_id)?);
        Ok(output)
    }
}

#[async_trait::async_trait]
impl CollectByBlock for StateDiffs {
    type Response = BlockTxsTraces;

    async fn extract(
        request: Params,
        source: Arc<Source>,
        schemas: Schemas,
    ) -> Result<Self::Response> {
        let include_txs = schemas.values().any(|x| x.has_column("transaction_hash"));
        source.fetcher.trace_block_state_diffs(request.block_number()? as u32, include_txs).await
    }

    fn transform(response: Self::Response, columns: &mut Self, schemas: &Schemas) -> Result<()> {
        process_state_diffs(response, columns, schemas)
    }
}

#[async_trait::async_trait]
impl CollectByTransaction for StateDiffs {
    type Response = BlockTxsTraces;

    async fn extract(
        request: Params,
        source: Arc<Source>,
        _schemas: Schemas,
    ) -> Result<Self::Response> {
        source.fetcher.trace_transaction_state_diffs(request.transaction_hash()?).await
    }

    fn transform(response: Self::Response, columns: &mut Self, schemas: &Schemas) -> Result<()> {
        process_state_diffs(response, columns, schemas)
    }
}

fn process_state_diffs(
    response: BlockTxsTraces,
    columns: &mut StateDiffs,
    schemas: &HashMap<Datatype, Table>,
) -> Result<()> {
    let StateDiffs(balances, codes, nonces, storages) = columns;
    balance_diffs::process_balance_diffs(&response, balances, schemas)?;
    code_diffs::process_code_diffs(&response, codes, schemas)?;
    nonce_diffs::process_nonce_diffs(&response, nonces, schemas)?;
    storage_diffs::process_storage_diffs(&response, storages, schemas)?;
    Ok(())
}
