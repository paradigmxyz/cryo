use crate::{
    conversions::ToVecHex, dataframes::SortableDataFrame, store, with_series, with_series_binary,
    with_series_option_u256, CollectByBlock, CollectError, ColumnData, ColumnEncoding, ColumnType,
    Datatype, Erc20Supplies, Params, Schemas, Source, Table, ToVecU8, U256Type,
};
use ethers::prelude::*;
use polars::prelude::*;
use std::collections::HashMap;

type Result<T> = ::core::result::Result<T, CollectError>;

type BlockErc20Supply = (u32, Vec<u8>, Option<U256>);

#[async_trait::async_trait]
impl CollectByBlock for Erc20Supplies {
    type Response = BlockErc20Supply;

    type Columns = Erc20SuppliesColumns;

    async fn extract(request: Params, source: Source, _schemas: Schemas) -> Result<Self::Response> {
        let signature: Vec<u8> = prefix_hex::decode("0x18160ddd").expect("Decoding failed");
        let mut call_data = signature.clone();
        call_data.extend(request.address());
        let block_number = request.ethers_block_number();
        let contract = request.ethers_contract();
        let output = source.fetcher.call2(contract, call_data, block_number).await.ok();
        let output = output.map(|x| x.to_vec().as_slice().into());
        Ok((request.block_number() as u32, request.contract(), output))
    }

    fn transform(response: Self::Response, columns: &mut Self::Columns, schemas: &Schemas) {
        let schema = schemas.get(&Datatype::Erc20Supplies).expect("missing schema");
        let (block, erc20, total_supply) = response;
        columns.n_rows += 1;
        store!(schema, columns, block_number, block);
        store!(schema, columns, erc20, erc20);
        store!(schema, columns, total_supply, total_supply);
    }
}

/// columns for transactions
#[cryo_to_df::to_df(Datatype::Erc20Supplies)]
#[derive(Default)]
pub struct Erc20SuppliesColumns {
    n_rows: u64,
    block_number: Vec<u32>,
    erc20: Vec<Vec<u8>>,
    total_supply: Vec<Option<U256>>,
}
