use crate::{
    conversions::ToVecHex, dataframes::SortableDataFrame, store, with_series, with_series_binary,
    with_series_option_u256, CollectByBlock, CollectError, ColumnData, ColumnEncoding, ColumnType,
    Datatype, Erc20Balances, Params, Schemas, Source, Table, ToVecU8, U256Type,
};
use ethers::prelude::*;
use polars::prelude::*;
use std::collections::HashMap;

type Result<T> = ::core::result::Result<T, CollectError>;

type BlockErc20AddressBalance = (u32, Vec<u8>, Vec<u8>, Option<U256>);

#[async_trait::async_trait]
impl CollectByBlock for Erc20Balances {
    type Response = BlockErc20AddressBalance;

    type Columns = Erc20BalancesColumns;

    async fn extract(request: Params, source: Source, _schemas: Schemas) -> Result<Self::Response> {
        let signature: Vec<u8> = prefix_hex::decode("0x70a08231").expect("Decoding failed");
        let mut call_data = signature.clone();
        call_data.extend(request.address());
        let block_number = request.ethers_block_number();
        let contract = request.ethers_contract();
        let balance = source.fetcher.call2(contract, call_data, block_number).await.ok();
        let balance = balance.map(|x| x.to_vec().as_slice().into());
        Ok((request.block_number() as u32, request.contract(), request.address(), balance))
    }

    fn transform(response: Self::Response, columns: &mut Self::Columns, schemas: &Schemas) {
        let schema = schemas.get(&Datatype::Erc20Balances).expect("missing schema");
        let (block, erc20, address, balance) = response;
        columns.n_rows += 1;
        store!(schema, columns, block_number, block);
        store!(schema, columns, erc20, erc20);
        store!(schema, columns, address, address);
        store!(schema, columns, balance, balance);
    }
}

/// columns for transactions
#[cryo_to_df::to_df(Datatype::Erc20Balances)]
#[derive(Default)]
pub struct Erc20BalancesColumns {
    n_rows: u64,
    block_number: Vec<u32>,
    erc20: Vec<Vec<u8>>,
    address: Vec<Vec<u8>>,
    balance: Vec<Option<U256>>,
}
