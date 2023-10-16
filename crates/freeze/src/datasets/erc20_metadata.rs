use crate::*;
use polars::prelude::*;
use std::collections::HashMap;

/// columns for transactions
#[cryo_to_df::to_df(Datatype::Erc20Metadata)]
#[derive(Default)]
pub struct Erc20Metadata {
    n_rows: u64,
    block_number: Vec<u32>,
    erc20: Vec<Vec<u8>>,
    name: Vec<Option<String>>,
    symbol: Vec<Option<String>>,
    decimals: Vec<Option<u32>>,
    chain_id: Vec<u64>,
}

#[async_trait::async_trait]
impl Dataset for Erc20Metadata {
    fn name() -> &'static str {
        "erc20_metadata"
    }

    fn default_sort() -> Vec<String> {
        vec!["symbol".to_string(), "block_number".to_string()]
    }

    fn default_blocks() -> Option<String> {
        Some("latest".to_string())
    }

    fn required_parameters() -> Vec<Dim> {
        vec![Dim::Address]
    }

    fn arg_aliases() -> Option<HashMap<Dim, Dim>> {
        Some([(Dim::Contract, Dim::Address)].into_iter().collect())
    }
}

type Result<T> = ::core::result::Result<T, CollectError>;
type BlockAddressNameSymbolDecimals = (u32, Vec<u8>, Option<String>, Option<String>, Option<u32>);

pub(crate) fn remove_control_characters(s: &str) -> String {
    let re = regex::Regex::new(r"[ \x00-\x1F\x7F]").unwrap();
    re.replace_all(s, "").to_string()
}

#[async_trait::async_trait]
impl CollectByBlock for Erc20Metadata {
    type Response = BlockAddressNameSymbolDecimals;

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

        // decimals
        let call_data = FUNCTION_ERC20_DECIMALS.clone();
        let output = source.fetcher.call2(address, call_data, block_number).await?;
        let decimals = bytes_to_u32(output).ok();

        Ok((request.block_number()? as u32, request.address()?, name, symbol, decimals))
    }

    fn transform(response: Self::Response, columns: &mut Self, schemas: &Schemas) -> Result<()> {
        let schema = schemas.get(&Datatype::Erc20Metadata).ok_or(err("schema not provided"))?;
        let (block, address, name, symbol, decimals) = response;
        columns.n_rows += 1;
        store!(schema, columns, block_number, block);
        store!(schema, columns, erc20, address);
        store!(schema, columns, name, name);
        store!(schema, columns, symbol, symbol);
        store!(schema, columns, decimals, decimals);
        Ok(())
    }
}

impl CollectByTransaction for Erc20Metadata {
    type Response = ();
}
