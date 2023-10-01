use crate::*;
use polars::prelude::*;
use std::collections::HashMap;

/// columns for transactions
#[cryo_to_df::to_df(Datatype::Transactions)]
#[derive(Default)]
pub struct Erc721MetadataColumns {
    n_rows: u64,
    block_number: Vec<u32>,
    erc721: Vec<Vec<u8>>,
    name: Vec<Option<String>>,
    symbol: Vec<Option<String>>,
}

impl Dataset for Erc721Metadata {
    fn datatype(&self) -> Datatype {
        Datatype::Erc721Metadata
    }

    fn name(&self) -> &'static str {
        "erc721_metadata"
    }

    fn column_types(&self) -> HashMap<&'static str, ColumnType> {
        HashMap::from_iter(vec![
            ("block_number", ColumnType::UInt32),
            ("erc20", ColumnType::Binary),
            ("name", ColumnType::String),
            ("symbol", ColumnType::String),
            ("chain_id", ColumnType::UInt64),
        ])
    }

    fn default_sort(&self) -> Vec<String> {
        vec!["symbol".to_string(), "block_number".to_string()]
    }
}

type Result<T> = ::core::result::Result<T, CollectError>;

type BlockAddressNameSymbol = (u32, Vec<u8>, Option<String>, Option<String>);

#[async_trait::async_trait]
impl CollectByBlock for Erc721Metadata {
    type Response = BlockAddressNameSymbol;

    type Columns = Erc721MetadataColumns;

    async fn extract(request: Params, source: Source, _schemas: Schemas) -> Result<Self::Response> {
        let block_number = request.ethers_block_number();
        let address = request.ethers_address();

        // name
        let call_data: Vec<u8> = prefix_hex::decode("0x06fdde03").expect("Decoding failed");
        let output = source.fetcher.call2(address, call_data, block_number).await?;
        let name = String::from_utf8(output.to_vec()).ok();

        // symbol
        let call_data: Vec<u8> = prefix_hex::decode("0x95d89b41").expect("Decoding failed");
        let output = source.fetcher.call2(address, call_data, block_number).await?;
        let symbol = String::from_utf8(output.to_vec()).ok();

        Ok((request.block_number() as u32, request.address(), name, symbol))
    }

    fn transform(response: Self::Response, columns: &mut Self::Columns, schemas: &Schemas) {
        let schema = schemas.get(&Datatype::Erc721Metadata).expect("missing schema");
        let (block, address, name, symbol) = response;
        columns.n_rows += 1;
        store!(schema, columns, block_number, block);
        store!(schema, columns, erc721, address);
        store!(schema, columns, name, name);
        store!(schema, columns, symbol, symbol);
    }
}
