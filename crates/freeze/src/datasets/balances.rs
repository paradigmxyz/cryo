use crate::*;
use ethers::prelude::*;
use polars::prelude::*;
use std::collections::HashMap;

/// columns for balances
#[cryo_to_df::to_df(Datatype::Balances)]
#[derive(Default)]
pub struct Balances {
    n_rows: usize,
    block_number: Vec<u32>,
    transaction_hash: Vec<Option<Vec<u8>>>,
    address: Vec<Vec<u8>>,
    balance: Vec<U256>,
}

#[async_trait::async_trait]
impl Dataset for Balances {
    fn name() -> &'static str {
        "balances"
    }

    fn default_sort() -> Vec<String> {
        vec!["block_number".to_string(), "address".to_string()]
    }
}

type Result<T> = ::core::result::Result<T, CollectError>;
type BlockTxAddressOutput = (u32, Option<Vec<u8>>, Vec<u8>, U256);

#[async_trait::async_trait]
impl CollectByBlock for Balances {
    type Response = BlockTxAddressOutput;

    fn parameters() -> Vec<Dim> {
        vec![Dim::BlockNumber, Dim::Address]
    }

    async fn extract(request: Params, source: Source, _schemas: Schemas) -> Result<Self::Response> {
        let address = request.address();
        let block_number = request.block_number() as u32;
        let balance =
            source.fetcher.get_balance(H160::from_slice(&address), block_number.into()).await?;
        Ok((block_number, None, address, balance))
    }

    fn transform(response: Self::Response, columns: &mut Self, schemas: &Schemas) {
        let schema = schemas.get(&Datatype::Balances).expect("missing schema");
        process_balance(columns, response, schema);
    }
}

#[async_trait::async_trait]
impl CollectByTransaction for Balances {
    type Response = BlockTxAddressOutput;

    fn parameters() -> Vec<Dim> {
        vec![Dim::TransactionHash, Dim::Address]
    }

    async fn extract(request: Params, source: Source, _schemas: Schemas) -> Result<Self::Response> {
        let tx = request.transaction_hash();
        let block_number = source.fetcher.get_transaction_block_number(tx.clone()).await?;
        let address = request.address();
        let balance =
            source.fetcher.get_balance(H160::from_slice(&address), block_number.into()).await?;
        Ok((block_number, Some(tx), address, balance))
    }

    fn transform(response: Self::Response, columns: &mut Self, schemas: &Schemas) {
        let schema = schemas.get(&Datatype::Balances).expect("missing schema");
        process_balance(columns, response, schema);
    }
}

fn process_balance(columns: &mut Balances, data: BlockTxAddressOutput, schema: &Table) {
    let (block, tx, address, balance) = data;
    columns.n_rows += 1;
    store!(schema, columns, block_number, block);
    store!(schema, columns, transaction_hash, tx);
    store!(schema, columns, address, address);
    store!(schema, columns, balance, balance);
}
