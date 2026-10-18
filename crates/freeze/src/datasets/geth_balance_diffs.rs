use crate::*;
use alloy::primitives::U256;
use polars::prelude::*;

/// columns for transactions
#[derive(Default, cryo_to_df::ToDataFrames)]
pub struct GethBalanceDiffs {
    pub(crate) n_rows: u64,
    pub(crate) block_number: Vec<Option<u32>>,
    pub(crate) transaction_index: Vec<Option<u64>>,
    pub(crate) transaction_hash: Vec<Option<RawBytes>>,
    pub(crate) address: Vec<RawBytes>,
    pub(crate) from_value: Vec<U256>,
    pub(crate) to_value: Vec<U256>,
    pub(crate) chain_id: Vec<u64>,
}

#[async_trait::async_trait]
impl Dataset for GethBalanceDiffs {}

#[async_trait::async_trait]
impl CollectByBlock for GethBalanceDiffs {
    type Response = <GethStateDiffs as CollectByBlock>::Response;

    async fn extract(request: Params, source: Arc<Source>, query: Arc<Query>) -> R<Self::Response> {
        <GethStateDiffs as CollectByBlock>::extract(request, source, query).await
    }

    fn transform(response: Self::Response, columns: &mut Self, query: &Arc<Query>) -> R<()> {
        let schemas = &query.schemas;
        geth_state_diffs::process_geth_diffs(&response, Some(columns), None, None, None, schemas)
    }
}

#[async_trait::async_trait]
impl CollectByTransaction for GethBalanceDiffs {
    type Response = <GethStateDiffs as CollectByTransaction>::Response;

    async fn extract(request: Params, source: Arc<Source>, query: Arc<Query>) -> R<Self::Response> {
        <GethStateDiffs as CollectByTransaction>::extract(request, source, query).await
    }

    fn transform(response: Self::Response, columns: &mut Self, query: &Arc<Query>) -> R<()> {
        let schemas = &query.schemas;
        geth_state_diffs::process_geth_diffs(&response, Some(columns), None, None, None, schemas)
    }
}
