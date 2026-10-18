use crate::*;
use polars::prelude::*;

/// columns for transactions
#[derive(Default, cryo_to_df::ToDataFrames)]
pub struct GethCodeDiffs {
    pub(crate) n_rows: u64,
    pub(crate) block_number: Vec<Option<u32>>,
    pub(crate) transaction_index: Vec<Option<u64>>,
    pub(crate) transaction_hash: Vec<Option<RawBytes>>,
    pub(crate) address: Vec<RawBytes>,
    pub(crate) from_value: Vec<RawBytes>,
    pub(crate) to_value: Vec<RawBytes>,
    pub(crate) chain_id: Vec<u64>,
}

#[async_trait::async_trait]
impl Dataset for GethCodeDiffs {}

#[async_trait::async_trait]
impl CollectByBlock for GethCodeDiffs {
    type Response = <GethStateDiffs as CollectByBlock>::Response;

    async fn extract(request: Params, source: Arc<Source>, query: Arc<Query>) -> R<Self::Response> {
        <GethStateDiffs as CollectByBlock>::extract(request, source, query).await
    }

    fn transform(response: Self::Response, columns: &mut Self, query: &Arc<Query>) -> R<()> {
        let schemas = &query.schemas;
        geth_state_diffs::process_geth_diffs(&response, None, Some(columns), None, None, schemas)
    }
}

#[async_trait::async_trait]
impl CollectByTransaction for GethCodeDiffs {
    type Response = <GethStateDiffs as CollectByTransaction>::Response;

    async fn extract(request: Params, source: Arc<Source>, query: Arc<Query>) -> R<Self::Response> {
        <GethStateDiffs as CollectByTransaction>::extract(request, source, query).await
    }

    fn transform(response: Self::Response, columns: &mut Self, query: &Arc<Query>) -> R<()> {
        let schemas = &query.schemas;
        geth_state_diffs::process_geth_diffs(&response, None, Some(columns), None, None, schemas)
    }
}
