use crate::*;
use ethers::prelude::*;
use polars::prelude::*;
use std::collections::HashMap;

/// columns for transactions
#[cryo_to_df::to_df(Datatype::GethBalanceDiffs)]
#[derive(Default)]
pub struct GethBalanceDiffs {
    pub(crate) n_rows: u64,
    pub(crate) block_number: Vec<Option<u32>>,
    pub(crate) transaction_index: Vec<Option<u64>>,
    pub(crate) transaction_hash: Vec<Option<Vec<u8>>>,
    pub(crate) address: Vec<Vec<u8>>,
    pub(crate) from_value: Vec<U256>,
    pub(crate) to_value: Vec<U256>,
    pub(crate) chain_id: Vec<u64>,
}

type Result<T> = ::core::result::Result<T, CollectError>;

#[async_trait::async_trait]
impl Dataset for GethBalanceDiffs {
    fn name() -> &'static str {
        "balance_diffs"
    }

    fn default_sort() -> Vec<String> {
        vec!["block_number".to_string(), "transaction_index".to_string()]
    }
}

#[async_trait::async_trait]
impl CollectByBlock for GethBalanceDiffs {
    type Response = <GethStateDiffs as CollectByBlock>::Response;

    async fn extract(
        request: Params,
        source: Arc<Source>,
        schemas: Schemas,
    ) -> Result<Self::Response> {
        <GethStateDiffs as CollectByBlock>::extract(request, source, schemas).await
    }

    fn transform(response: Self::Response, columns: &mut Self, schemas: &Schemas) -> Result<()> {
        geth_state_diffs::process_geth_diffs(&response, Some(columns), None, None, None, schemas)
    }
}

#[async_trait::async_trait]
impl CollectByTransaction for GethBalanceDiffs {
    type Response = <GethStateDiffs as CollectByTransaction>::Response;

    async fn extract(
        request: Params,
        source: Arc<Source>,
        schemas: Schemas,
    ) -> Result<Self::Response> {
        <GethStateDiffs as CollectByTransaction>::extract(request, source, schemas).await
    }

    fn transform(response: Self::Response, columns: &mut Self, schemas: &Schemas) -> Result<()> {
        geth_state_diffs::process_geth_diffs(&response, Some(columns), None, None, None, schemas)
    }
}
