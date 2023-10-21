use crate::*;
use ethers::prelude::*;
use polars::prelude::*;

/// columns for transactions
#[cryo_to_df::to_df(Datatype::Erc20Supplies)]
#[derive(Default)]
pub struct Erc20Supplies {
    n_rows: u64,
    block_number: Vec<u32>,
    erc20: Vec<Vec<u8>>,
    total_supply: Vec<Option<U256>>,
    chain_id: Vec<u64>,
}

#[async_trait::async_trait]
impl Dataset for Erc20Supplies {
    fn default_sort() -> Option<Vec<&'static str>> {
        Some(vec!["erc20", "block_number"])
    }

    fn required_parameters() -> Vec<Dim> {
        vec![Dim::Address]
    }

    fn arg_aliases() -> Option<std::collections::HashMap<Dim, Dim>> {
        Some([(Dim::Contract, Dim::Address)].into_iter().collect())
    }

    fn default_blocks() -> Option<String> {
        Some("latest".to_string())
    }
}

#[async_trait::async_trait]
impl CollectByBlock for Erc20Supplies {
    type Response = (u32, Vec<u8>, Option<U256>);

    async fn extract(request: Params, source: Arc<Source>, _: Arc<Query>) -> R<Self::Response> {
        let signature: Vec<u8> = FUNCTION_ERC20_TOTAL_SUPPLY.clone();
        let mut call_data = signature.clone();
        call_data.extend(request.address()?);
        let block_number = request.ethers_block_number()?;
        let contract = request.ethers_address()?;
        let output = source.fetcher.call2(contract, call_data, block_number).await.ok();
        let output = output.map(|x| x.to_vec().as_slice().into());
        Ok((request.block_number()? as u32, request.address()?, output))
    }

    fn transform(response: Self::Response, columns: &mut Self, query: &Arc<Query>) -> R<()> {
        let schema = query.schemas.get_schema(&Datatype::Erc20Supplies)?;
        let (block, erc20, total_supply) = response;
        columns.n_rows += 1;
        store!(schema, columns, block_number, block);
        store!(schema, columns, erc20, erc20);
        store!(schema, columns, total_supply, total_supply);
        Ok(())
    }
}

impl CollectByTransaction for Erc20Supplies {
    type Response = ();
}
