use crate::*;
use polars::prelude::*;

/// columns for transactions
#[cryo_to_df::to_df(Datatype::GethStorageDiffs)]
#[derive(Default)]
pub struct GethStorageDiffs {
    pub(crate) n_rows: u64,
    pub(crate) block_number: Vec<Option<u32>>,
    pub(crate) transaction_index: Vec<Option<u64>>,
    pub(crate) transaction_hash: Vec<Option<Vec<u8>>>,
    pub(crate) address: Vec<Vec<u8>>,
    pub(crate) slot: Vec<Vec<u8>>,
    pub(crate) from_value: Vec<Vec<u8>>,
    pub(crate) to_value: Vec<Vec<u8>>,
    pub(crate) chain_id: Vec<u64>,
}

impl Dataset for GethStorageDiffs {}

#[async_trait::async_trait]
impl CollectByBlock for GethStorageDiffs {
    type Response = <GethStateDiffs as CollectByBlock>::Response;

    async fn extract(request: Params, source: Arc<Source>, query: Arc<Query>) -> R<Self::Response> {
        <GethStateDiffs as CollectByBlock>::extract(request, source, query).await
    }

    fn transform(response: Self::Response, columns: &mut Self, query: &Arc<Query>) -> R<()> {
        let schemas = &query.schemas;
        geth_state_diffs::process_geth_diffs(&response, None, None, None, Some(columns), schemas)
    }
}

#[async_trait::async_trait]
impl CollectByTransaction for GethStorageDiffs {
    type Response = <GethStateDiffs as CollectByTransaction>::Response;

    async fn extract(request: Params, source: Arc<Source>, query: Arc<Query>) -> R<Self::Response> {
        <GethStateDiffs as CollectByTransaction>::extract(request, source, query).await
    }

    fn transform(response: Self::Response, columns: &mut Self, query: &Arc<Query>) -> R<()> {
        let schemas = &query.schemas;
        geth_state_diffs::process_geth_diffs(&response, None, None, None, Some(columns), schemas)
    }
}
