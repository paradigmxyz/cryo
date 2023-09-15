// required args:: address

use crate::{types::Storages, ColumnType, Dataset, Datatype};
use std::collections::HashMap;

use ethers::prelude::*;
use polars::prelude::*;
use tokio::{sync::mpsc, task};

use crate::{
    dataframes::SortableDataFrame,
    types::{
        conversions::ToVecHex, AddressChunk, BlockChunk, CollectError, RowFilter, SlotChunk,
        Source, Table,
    },
    with_series, with_series_binary,
};

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

    fn default_columns(&self) -> Vec<&'static str> {
        vec!["block_number", "address", "slot", "value", "chain_id"]
    }

    fn default_sort(&self) -> Vec<String> {
        vec!["block_number".to_string(), "address".to_string(), "slot".to_string()]
    }

    async fn collect_block_chunk(
        &self,
        chunk: &BlockChunk,
        source: &Source,
        schema: &Table,
        filter: Option<&RowFilter>,
    ) -> Result<DataFrame, CollectError> {
        let (address_chunks, slot_chunks) = match filter {
            Some(filter) => (filter.address_chunks()?, filter.slot_chunks()?),
            _ => return Err(CollectError::CollectError("must specify RowFilter".to_string())),
        };
        let rx = fetch_slots(vec![chunk], address_chunks, slot_chunks, source).await;
        slots_to_df(rx, schema, source.chain_id).await
    }
}

pub(crate) type BlockAddressSlot = (u64, Vec<u8>, Vec<u8>, Vec<u8>);

async fn fetch_slots(
    block_chunks: Vec<&BlockChunk>,
    address_chunks: Vec<AddressChunk>,
    slot_chunks: Vec<SlotChunk>,
    source: &Source,
) -> mpsc::Receiver<Result<BlockAddressSlot, CollectError>> {
    let (tx, rx) = mpsc::channel(100);

    for block_chunk in block_chunks {
        for number in block_chunk.numbers() {
            for address_chunk in &address_chunks {
                for address in address_chunk.values().iter() {
                    for slot_chunk in &slot_chunks {
                        for slot in slot_chunk.values().iter() {
                            let address = address.clone();
                            let address_h160 = H160::from_slice(&address);
                            let slot = slot.clone();
                            let slot_h256 = H256::from_slice(&slot);
                            let tx = tx.clone();
                            let source = source.clone();
                            task::spawn(async move {
                                let result = source
                                    .fetcher
                                    .get_storage_at(address_h160, slot_h256, number.into())
                                    .await;
                                let result = match result {
                                    Ok(value) => {
                                        Ok((number, address, slot, value.as_bytes().to_vec()))
                                    }
                                    Err(e) => Err(e),
                                };
                                match tx.send(result).await {
                                    Ok(_) => {}
                                    Err(tokio::sync::mpsc::error::SendError(_e)) => {
                                        eprintln!("send error, try using a rate limit with --requests-per-second or limiting max concurrency with --max-concurrent-requests");
                                        std::process::exit(1)
                                    }
                                }
                            });
                        }
                    }
                }
            }
        }
    }

    rx
}

async fn slots_to_df(
    mut stream: mpsc::Receiver<Result<BlockAddressSlot, CollectError>>,
    schema: &Table,
    chain_id: u64,
) -> Result<DataFrame, CollectError> {
    // initialize
    let mut columns = SlotColumns::default();

    // parse stream of blocks
    while let Some(message) = stream.recv().await {
        match message {
            Ok(block_address_slot) => {
                columns.process_slots(block_address_slot, schema);
            }
            Err(e) => {
                println!("{:?}", e);
                return Err(CollectError::TooManyRequestsError)
            }
        }
    }

    // convert to dataframes
    columns.create_df(schema, chain_id)
}

#[derive(Default)]
struct SlotColumns {
    n_rows: usize,
    block_number: Vec<u32>,
    address: Vec<Vec<u8>>,
    slot: Vec<Vec<u8>>,
    value: Vec<Vec<u8>>,
}

impl SlotColumns {
    fn process_slots(&mut self, block_address_slot: BlockAddressSlot, schema: &Table) {
        let (block, address, slot, value) = block_address_slot;
        self.n_rows += 1;
        if schema.has_column("block_number") {
            self.block_number.push(block as u32);
        }
        if schema.has_column("address") {
            self.address.push(address);
        }
        if schema.has_column("slot") {
            self.slot.push(slot);
        }
        if schema.has_column("value") {
            self.value.push(value);
        }
    }

    fn create_df(self, schema: &Table, chain_id: u64) -> Result<DataFrame, CollectError> {
        let mut cols = Vec::with_capacity(schema.columns().len());
        with_series!(cols, "block_number", self.block_number, schema);
        with_series_binary!(cols, "address", self.address, schema);
        with_series_binary!(cols, "slot", self.slot, schema);
        with_series_binary!(cols, "value", self.value, schema);
        with_series!(cols, "chain_id", vec![chain_id; self.n_rows], schema);

        DataFrame::new(cols).map_err(CollectError::PolarsError).sort_by_schema(schema)
    }
}
