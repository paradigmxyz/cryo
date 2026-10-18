use super::erc20_metadata::remove_control_characters;
use crate::*;
use alloy::sol_types::SolCall;
use polars::prelude::*;

/// columns for transactions
#[derive(Default, cryo_to_df::ToDataFrames)]
pub struct Erc721Metadata {
    n_rows: u64,
    block_number: Vec<u32>,
    erc721: Vec<RawBytes>,
    name: Vec<Option<String>>,
    symbol: Vec<Option<String>>,
    chain_id: Vec<u64>,
}

impl Dataset for Erc721Metadata {
    fn default_sort() -> Option<Vec<&'static str>> {
        Some(vec!["symbol", "block_number"])
    }

    fn required_parameters() -> Vec<Dim> {
        vec![Dim::Address]
    }

    fn arg_aliases() -> Option<std::collections::HashMap<Dim, Dim>> {
        Some([(Dim::Contract, Dim::Address)].into_iter().collect())
    }
}

#[async_trait::async_trait]
impl CollectByBlock for Erc721Metadata {
    type Response = (u32, RawBytes, Option<String>, Option<String>);

    async fn extract(request: Params, source: Arc<Source>, _: Arc<Query>) -> R<Self::Response> {
        let block_number = request.ethers_block_number()?;
        let address = request.ethers_address()?;

        // name
        let call_data = ERC721::nameCall::SELECTOR.to_vec();
        let name = match source.call2(address, call_data, block_number).await {
            Ok(output) => {
                String::from_utf8(output.to_vec()).ok().map(|s| remove_control_characters(&s))
            }
            Err(_) => None,
        };

        // symbol
        let call_data = ERC721::symbolCall::SELECTOR.to_vec();
        let symbol = match source.call2(address, call_data, block_number).await {
            Ok(output) => {
                String::from_utf8(output.to_vec()).ok().map(|s| remove_control_characters(&s))
            }
            Err(_) => None,
        };

        Ok((request.block_number()? as u32, request.address()?, name, symbol))
    }

    fn transform(response: Self::Response, columns: &mut Self, query: &Arc<Query>) -> R<()> {
        let schema = query.schemas.get_schema(&Datatype::Erc721Metadata)?;
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
