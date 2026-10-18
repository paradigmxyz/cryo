use crate::*;
use alloy::primitives::{Address, U256};
use polars::prelude::*;

/// columns for balances
#[derive(Default, cryo_to_df::ToDataFrames)]
pub struct Slots {
    n_rows: usize,
    block_number: Vec<u32>,
    address: Vec<RawBytes>,
    slot: Vec<RawBytes>,
    value: Vec<RawBytes>,
    chain_id: Vec<u64>,
}

#[async_trait::async_trait]
impl Dataset for Slots {
    fn default_sort() -> Option<Vec<&'static str>> {
        Some(vec!["block_number", "address", "slot"])
    }

    fn required_parameters() -> Vec<Dim> {
        vec![Dim::Address, Dim::Slot]
    }

    fn aliases() -> Vec<&'static str> {
        vec!["storages"]
    }

    fn arg_aliases() -> Option<std::collections::HashMap<Dim, Dim>> {
        Some([(Dim::Contract, Dim::Address)].into_iter().collect())
    }

    fn default_blocks() -> Option<String> {
        Some("latest".to_string())
    }
}

type BlockTxAddressOutput = (u32, Option<RawBytes>, RawBytes, RawBytes, RawBytes);

#[async_trait::async_trait]
impl CollectByBlock for Slots {
    type Response = BlockTxAddressOutput;

    async fn extract(request: Params, source: Arc<Source>, _: Arc<Query>) -> R<Self::Response> {
        let address = request.address()?;
        let block_number = request.block_number()? as u32;
        let slot = request.slot()?;
        let output = source
            .get_storage_at(
                Address::from_slice(&address),
                U256::from_be_slice(&slot),
                block_number.into(),
            )
            .await?;
        Ok((block_number, None, address, slot, output.to_vec_u8()))
    }

    fn transform(response: Self::Response, columns: &mut Self, query: &Arc<Query>) -> R<()> {
        let schema = query.schemas.get_schema(&Datatype::Slots)?;
        process_slot(columns, response, schema)
    }
}

#[async_trait::async_trait]
impl CollectByTransaction for Slots {
    type Response = ();
}

fn process_slot(columns: &mut Slots, data: BlockTxAddressOutput, schema: &Table) -> R<()> {
    let (block, _tx, address, slot, output) = data;
    columns.n_rows += 1;
    store!(schema, columns, block_number, block);
    store!(schema, columns, address, address);
    store!(schema, columns, slot, slot);
    store!(schema, columns, value, output);
    Ok(())
}
