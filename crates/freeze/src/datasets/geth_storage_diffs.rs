use crate::*;
use polars::prelude::*;
use std::collections::HashMap;

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

#[async_trait::async_trait]
impl Dataset for GethStorageDiffs {
    fn name() -> &'static str {
        "geth_storage_diffs"
    }

    fn default_sort() -> Vec<String> {
        vec!["block_number".to_string(), "transaction_index".to_string()]
    }
}

type Result<T> = ::core::result::Result<T, CollectError>;

#[async_trait::async_trait]
impl CollectByBlock for GethStorageDiffs {
    type Response = <GethStateDiffs as CollectByBlock>::Response;

    async fn extract(
        request: Params,
        source: Arc<Source>,
        schemas: Schemas,
    ) -> Result<Self::Response> {
        <GethStateDiffs as CollectByBlock>::extract(request, source, schemas).await
    }

    fn transform(response: Self::Response, columns: &mut Self, schemas: &Schemas) -> Result<()> {
        geth_state_diffs::process_geth_diffs(&response, None, None, None, Some(columns), schemas)
    }
}

#[async_trait::async_trait]
impl CollectByTransaction for GethStorageDiffs {
    type Response = <GethStateDiffs as CollectByTransaction>::Response;

    async fn extract(
        request: Params,
        source: Arc<Source>,
        schemas: Schemas,
    ) -> Result<Self::Response> {
        <GethStateDiffs as CollectByTransaction>::extract(request, source, schemas).await
    }

    fn transform(response: Self::Response, columns: &mut Self, schemas: &Schemas) -> Result<()> {
        geth_state_diffs::process_geth_diffs(&response, None, None, None, Some(columns), schemas)
    }
}
