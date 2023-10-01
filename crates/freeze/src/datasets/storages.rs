use crate::*;
use ethers::prelude::*;
use polars::prelude::*;
use std::collections::HashMap;

/// columns for balances
#[cryo_to_df::to_df(Datatype::Storages)]
#[derive(Default)]
pub struct StorageColumns {
    n_rows: usize,
    block_number: Vec<u32>,
    transaction_hash: Vec<Option<Vec<u8>>>,
    address: Vec<Vec<u8>>,
    slot: Vec<Vec<u8>>,
    value: Vec<Vec<u8>>,
}

#[async_trait::async_trait]
impl Dataset for Storages {
    fn datatype(&self) -> Datatype {
        Datatype::Storages
    }

    fn name(&self) -> &'static str {
        "storages"
    }

    fn column_types(&self) -> HashMap<&'static str, ColumnType> {
        HashMap::from_iter(vec![
            ("block_number", ColumnType::UInt32),
            ("address", ColumnType::Binary),
            ("slot", ColumnType::Binary),
            ("value", ColumnType::Binary),
            ("chain_id", ColumnType::UInt64),
        ])
    }

    fn default_sort(&self) -> Vec<String> {
        vec!["block_number".to_string(), "address".to_string(), "slot".to_string()]
    }
}

type Result<T> = ::core::result::Result<T, CollectError>;
type BlockTxAddressOutput = (u32, Option<Vec<u8>>, Vec<u8>, Vec<u8>, Vec<u8>);

#[async_trait::async_trait]
impl CollectByBlock for Storages {
    type Response = BlockTxAddressOutput;

    type Columns = StorageColumns;

    fn block_parameters() -> Vec<ChunkDim> {
        vec![ChunkDim::BlockNumber, ChunkDim::Address, ChunkDim::Slot]
    }

    async fn extract(request: Params, source: Source, _schemas: Schemas) -> Result<Self::Response> {
        let address = request.address();
        let block_number = request.block_number() as u32;
        let slot = request.slot();
        let output = source
            .fetcher
            .get_storage_at(
                H160::from_slice(&address),
                H256::from_slice(&slot),
                block_number.into(),
            )
            .await?;
        Ok((block_number, None, address, slot, output.as_bytes().to_vec()))
    }

    fn transform(response: Self::Response, columns: &mut Self::Columns, schemas: &Schemas) {
        let schema = schemas.get(&Datatype::Storages).expect("missing schema");
        process_nonce(columns, response, schema);
    }
}

#[async_trait::async_trait]
impl CollectByTransaction for Storages {
    type Response = BlockTxAddressOutput;

    type Columns = StorageColumns;

    fn transaction_parameters() -> Vec<ChunkDim> {
        vec![ChunkDim::TransactionHash, ChunkDim::Address]
    }

    async fn extract(request: Params, source: Source, _schemas: Schemas) -> Result<Self::Response> {
        let tx = request.transaction_hash();
        let block_number = source.fetcher.get_transaction_block_number(tx.clone()).await?;
        let address = request.address();
        let slot = request.slot();
        let output = source
            .fetcher
            .get_storage_at(
                H160::from_slice(&address),
                H256::from_slice(&slot),
                block_number.into(),
            )
            .await?;
        Ok((block_number, None, address, slot, output.as_bytes().to_vec()))
    }

    fn transform(response: Self::Response, columns: &mut Self::Columns, schemas: &Schemas) {
        let schema = schemas.get(&Datatype::Storages).expect("missing schema");
        process_nonce(columns, response, schema);
    }
}

fn process_nonce(columns: &mut StorageColumns, data: BlockTxAddressOutput, schema: &Table) {
    let (block, tx, address, slot, output) = data;
    columns.n_rows += 1;
    store!(schema, columns, block_number, block);
    store!(schema, columns, transaction_hash, tx);
    store!(schema, columns, address, address);
    store!(schema, columns, slot, slot);
    store!(schema, columns, value, output);
}
