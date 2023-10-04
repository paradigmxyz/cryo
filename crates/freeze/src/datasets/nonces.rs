use crate::*;
use ethers::prelude::*;
use polars::prelude::*;
use std::collections::HashMap;

/// columns for balances
#[cryo_to_df::to_df(Datatype::Nonces)]
#[derive(Default)]
pub struct Nonces {
    n_rows: usize,
    block_number: Vec<u32>,
    transaction_hash: Vec<Option<Vec<u8>>>,
    address: Vec<Vec<u8>>,
    nonce: Vec<u64>,
}

#[async_trait::async_trait]
impl Dataset for Nonces {
    fn name() -> &'static str {
        "nonces"
    }

    fn default_sort() -> Vec<String> {
        vec!["block_number".to_string(), "address".to_string()]
    }
}

type Result<T> = ::core::result::Result<T, CollectError>;
type BlockTxAddressOutput = (u32, Option<Vec<u8>>, Vec<u8>, u64);

#[async_trait::async_trait]
impl CollectByBlock for Nonces {
    type Response = BlockTxAddressOutput;

    fn block_parameters() -> Vec<ChunkDim> {
        vec![ChunkDim::BlockNumber, ChunkDim::Address]
    }

    async fn extract(request: Params, source: Source, _schemas: Schemas) -> Result<Self::Response> {
        let address = request.address();
        let block_number = request.block_number() as u32;
        let output = source
            .fetcher
            .get_transaction_count(H160::from_slice(&address), block_number.into())
            .await?;
        Ok((block_number, None, address, output.as_u64()))
    }

    fn transform(response: Self::Response, columns: &mut Self, schemas: &Schemas) {
        let schema = schemas.get(&Datatype::Nonces).expect("missing schema");
        process_nonce(columns, response, schema);
    }
}

#[async_trait::async_trait]
impl CollectByTransaction for Nonces {
    type Response = BlockTxAddressOutput;

    fn transaction_parameters() -> Vec<ChunkDim> {
        vec![ChunkDim::TransactionHash, ChunkDim::Address]
    }

    async fn extract(request: Params, source: Source, _schemas: Schemas) -> Result<Self::Response> {
        let tx = request.transaction_hash();
        let block_number = source.fetcher.get_transaction_block_number(tx.clone()).await?;
        let address = request.address();
        let output = source
            .fetcher
            .get_transaction_count(H160::from_slice(&address), block_number.into())
            .await?;

        Ok((block_number, Some(tx), address, output.as_u64()))
    }

    fn transform(response: Self::Response, columns: &mut Self, schemas: &Schemas) {
        let schema = schemas.get(&Datatype::Nonces).expect("missing schema");
        process_nonce(columns, response, schema);
    }
}

fn process_nonce(columns: &mut Nonces, data: BlockTxAddressOutput, schema: &Table) {
    let (block, tx, address, output) = data;
    columns.n_rows += 1;
    store!(schema, columns, block_number, block);
    store!(schema, columns, transaction_hash, tx);
    store!(schema, columns, address, address);
    store!(schema, columns, nonce, output);
}
