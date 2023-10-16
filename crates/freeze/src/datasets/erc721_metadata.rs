use super::erc20_metadata::remove_control_characters;
use crate::*;
use polars::prelude::*;
use std::collections::HashMap;

/// columns for transactions
#[cryo_to_df::to_df(Datatype::Erc721Metadata)]
#[derive(Default)]
pub struct Erc721Metadata {
    n_rows: u64,
    block_number: Vec<u32>,
    erc721: Vec<Vec<u8>>,
    name: Vec<Option<String>>,
    symbol: Vec<Option<String>>,
    chain_id: Vec<u64>,
}

impl Dataset for Erc721Metadata {
    fn name() -> &'static str {
        "erc721_metadata"
    }

    fn default_sort() -> Vec<String> {
        vec!["symbol".to_string(), "block_number".to_string()]
    }

    fn required_parameters() -> Vec<Dim> {
        vec![Dim::Address]
    }

    fn arg_aliases() -> Option<HashMap<Dim, Dim>> {
        Some([(Dim::Contract, Dim::Address)].into_iter().collect())
    }
}

type Result<T> = ::core::result::Result<T, CollectError>;

type BlockAddressNameSymbol = (u32, Vec<u8>, Option<String>, Option<String>);

#[async_trait::async_trait]
impl CollectByBlock for Erc721Metadata {
    type Response = BlockAddressNameSymbol;

    async fn extract(
        request: Params,
        source: Arc<Source>,
        _schemas: Schemas,
    ) -> Result<Self::Response> {
        let block_number = request.ethers_block_number()?;
        let address = request.ethers_address()?;

        // name
        let call_data = FUNCTION_ERC20_NAME.clone();
        let output = source.fetcher.call2(address, call_data, block_number).await?;
        let name = String::from_utf8(output.to_vec()).ok().map(|s| remove_control_characters(&s));

        // symbol
        let call_data = FUNCTION_ERC20_SYMBOL.clone();
        let output = source.fetcher.call2(address, call_data, block_number).await?;
        let symbol = String::from_utf8(output.to_vec()).ok().map(|s| remove_control_characters(&s));

        Ok((request.block_number()? as u32, request.address()?, name, symbol))
    }

    fn transform(response: Self::Response, columns: &mut Self, schemas: &Schemas) -> Result<()> {
        let schema = schemas.get(&Datatype::Erc721Metadata).ok_or(err("schema not provided"))?;
        let (block, address, name, symbol) = response;
        columns.n_rows += 1;
        store!(schema, columns, block_number, block);
        store!(schema, columns, erc721, address);
        store!(schema, columns, name, name);
        store!(schema, columns, symbol, symbol);
        Ok(())
    }
}

impl CollectByTransaction for Erc721Metadata {
    type Response = ();
}
