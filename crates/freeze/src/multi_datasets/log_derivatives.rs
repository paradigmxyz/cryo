use crate::*;
use ethers::prelude::*;
use polars::prelude::*;
use std::collections::HashMap;

/// LogDerivatives
#[derive(Default)]
pub struct LogDerivatives(
    logs::Logs,
    erc20_transfers::Erc20Transfers,
    erc721_transfers::Erc721Transfers,
);

impl ToDataFrames for LogDerivatives {
    fn create_dfs(
        self,
        schemas: &HashMap<Datatype, Table>,
        chain_id: u64,
    ) -> R<HashMap<Datatype, DataFrame>> {
        let LogDerivatives(logs, erc20_transfers, erc721_transfers) = self;
        let mut output = HashMap::new();
        if schemas.contains_key(&Datatype::Logs) {
            output.extend(log.create_dfs(schemas, chain_id)?);
        }
        if schemas.contains_key(&Datatype::Erc20Transfers) {
            output.extend(erc20_transfers.create_dfs(schemas, chain_id)?);
        }
        if schemas.contains_key(&Datatype::Erc721Transfers) {
            output.extend(erc721_transfers.create_dfs(schemas, chain_id)?);
        }
        Ok(output)
    }
}

#[async_trait::async_trait]
impl CollectByBlock for LogDerivatives {
    type Response = Vec<Log>;

    async fn extract(request: Params, source: Arc<Source>, _: Arc<Query>) -> R<Self::Response> {
        let filter = if schemas.contains(Datatype::Logs) {
            Filter { ..request.ethers_log_filter()? };
        } else {
            // erc20 and erc721 share same event signature
            let topics = [Some(ValueOrArray::Value(Some(*EVENT_ERC20_TRANSFER))), None, None, None];
            Filter { topics, ..request.ethers_log_filter()? };
        };
        let logs = source.fetcher.get_logs(&filter).await?;
        source.fetcher.trace_block(logs).await
    }

    fn transform(response: Self::Response, columns: &mut Self, query: &Arc<Query>) -> R<()> {
        process_log_derivatives(response, columns, &query.schemas)
    }
}

#[async_trait::async_trait]
impl CollectByTransaction for LogDerivatives {
    type Response = Vec<Trace>;

    async fn extract(request: Params, source: Arc<Source>, _: Arc<Query>) -> R<Self::Response> {
        let logs = source.fetcher.get_transaction_logs(request.transaction_hash()?).await?;
        Ok(logs.into_iter().filter(is_erc20_transfer).collect())
    }

    fn transform(response: Self::Response, columns: &mut Self, query: &Arc<Query>) -> R<()> {
        process_log_derivatives(response, columns, &query.schemas)
    }
}

fn process_log_derivatives(
    response: Vec<Trace>,
    columns: &mut LogDerivatives,
    schemas: &HashMap<Datatype, Table>,
) -> R<()> {
    let LogDerivatives(contracts, native_transfers, traces) = columns;
    if schemas.contains_key(&Datatype::Contracts) {
        contracts::process_contracts(&response, contracts, schemas)?;
    }
    if schemas.contains_key(&Datatype::NativeTransfers) {
        native_transfers::process_native_transfers(&response, native_transfers, schemas)?;
    }
    if schemas.contains_key(&Datatype::Traces) {
        traces::process_traces(&response, traces, schemas)?;
    }
    Ok(())
}
