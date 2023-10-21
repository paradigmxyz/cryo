use crate::*;
use ethers::prelude::*;
use polars::prelude::*;
use std::collections::HashMap;

/// CallTraceDerivatives
#[derive(Default)]
pub struct CallTraceDerivatives(
    contracts::Contracts,
    native_transfers::NativeTransfers,
    traces::Traces,
);

impl ToDataFrames for CallTraceDerivatives {
    fn create_dfs(
        self,
        schemas: &HashMap<Datatype, Table>,
        chain_id: u64,
    ) -> R<HashMap<Datatype, DataFrame>> {
        let CallTraceDerivatives(contracts, native_transfers, traces) = self;
        let mut output = HashMap::new();
        if schemas.contains_key(&Datatype::Contracts) {
            output.extend(contracts.create_dfs(schemas, chain_id)?);
        }
        if schemas.contains_key(&Datatype::NativeTransfers) {
            output.extend(native_transfers.create_dfs(schemas, chain_id)?);
        }
        if schemas.contains_key(&Datatype::Traces) {
            output.extend(traces.create_dfs(schemas, chain_id)?);
        }
        Ok(output)
    }
}

#[async_trait::async_trait]
impl CollectByBlock for CallTraceDerivatives {
    type Response = Vec<Trace>;

    async fn extract(request: Params, source: Arc<Source>, _: Arc<Query>) -> R<Self::Response> {
        source.fetcher.trace_block(request.block_number()?.into()).await
    }

    fn transform(response: Self::Response, columns: &mut Self, query: &Arc<Query>) -> R<()> {
        let traces =
            if query.exclude_failed { traces::filter_failed_traces(response) } else { response };
        process_call_trace_derivatives(traces, columns, &query.schemas)
    }
}

#[async_trait::async_trait]
impl CollectByTransaction for CallTraceDerivatives {
    type Response = Vec<Trace>;

    async fn extract(request: Params, source: Arc<Source>, _: Arc<Query>) -> R<Self::Response> {
        source.fetcher.trace_transaction(request.ethers_transaction_hash()?).await
    }

    fn transform(response: Self::Response, columns: &mut Self, query: &Arc<Query>) -> R<()> {
        let traces =
            if query.exclude_failed { traces::filter_failed_traces(response) } else { response };
        process_call_trace_derivatives(traces, columns, &query.schemas)
    }
}

fn process_call_trace_derivatives(
    response: Vec<Trace>,
    columns: &mut CallTraceDerivatives,
    schemas: &HashMap<Datatype, Table>,
) -> R<()> {
    let CallTraceDerivatives(contracts, native_transfers, traces) = columns;
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
