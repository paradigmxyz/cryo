use crate::{
    conversions::ToVecHex, dataframes::SortableDataFrame, store, with_series, with_series_binary,
    ChunkDim, Codes, CollectByBlock, CollectByTransaction, CollectError, ColumnData, ColumnType,
    Dataset, Datatype, Params, Schemas, Source, Table,
};
use ethers::prelude::*;
use polars::prelude::*;
use std::collections::HashMap;

/// columns for balances
#[cryo_to_df::to_df(Datatype::Codes)]
#[derive(Default)]
pub struct CodeColumns {
    n_rows: usize,
    block_number: Vec<u32>,
    transaction_hash: Vec<Option<Vec<u8>>>,
    address: Vec<Vec<u8>>,
    code: Vec<Vec<u8>>,
}

#[async_trait::async_trait]
impl Dataset for Codes {
    fn datatype(&self) -> Datatype {
        Datatype::Codes
    }

    fn name(&self) -> &'static str {
        "codes"
    }

    fn column_types(&self) -> HashMap<&'static str, ColumnType> {
        HashMap::from_iter(vec![
            ("block_number", ColumnType::UInt32),
            ("address", ColumnType::Binary),
            ("code", ColumnType::Binary),
            ("chain_id", ColumnType::UInt64),
        ])
    }

    fn default_sort(&self) -> Vec<String> {
        vec!["block_number".to_string(), "address".to_string()]
    }
}

type Result<T> = ::core::result::Result<T, CollectError>;
type BlockTxAddressOutput = (u32, Option<Vec<u8>>, Vec<u8>, Vec<u8>);

#[async_trait::async_trait]
impl CollectByBlock for Codes {
    type Response = BlockTxAddressOutput;

    type Columns = CodeColumns;

    fn block_parameters() -> Vec<ChunkDim> {
        vec![ChunkDim::BlockNumber, ChunkDim::Address]
    }

    async fn extract(request: Params, source: Source, _schemas: Schemas) -> Result<Self::Response> {
        let address = request.address();
        let block_number = request.block_number() as u32;
        let output =
            source.fetcher.get_code(H160::from_slice(&address), block_number.into()).await?;
        Ok((block_number, None, address, output.to_vec()))
    }

    fn transform(response: Self::Response, columns: &mut Self::Columns, schemas: &Schemas) {
        let schema = schemas.get(&Datatype::Codes).expect("missing schema");
        process_nonce(columns, response, schema);
    }
}

#[async_trait::async_trait]
impl CollectByTransaction for Codes {
    type Response = BlockTxAddressOutput;

    type Columns = CodeColumns;

    fn transaction_parameters() -> Vec<ChunkDim> {
        vec![ChunkDim::TransactionHash, ChunkDim::Address]
    }

    async fn extract(request: Params, source: Source, _schemas: Schemas) -> Result<Self::Response> {
        let tx = request.transaction_hash();
        let block_number = source.fetcher.get_transaction_block_number(tx.clone()).await?;
        let address = request.address();
        let output =
            source.fetcher.get_code(H160::from_slice(&address), block_number.into()).await?;
        Ok((block_number, Some(tx), address, output.to_vec()))
    }

    fn transform(response: Self::Response, columns: &mut Self::Columns, schemas: &Schemas) {
        let schema = schemas.get(&Datatype::Codes).expect("missing schema");
        process_nonce(columns, response, schema);
    }
}

fn process_nonce(columns: &mut CodeColumns, data: BlockTxAddressOutput, schema: &Table) {
    let (block, tx, address, output) = data;
    columns.n_rows += 1;
    store!(schema, columns, block_number, block);
    store!(schema, columns, transaction_hash, tx);
    store!(schema, columns, address, address);
    store!(schema, columns, code, output);
}
