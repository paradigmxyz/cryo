use crate::*;
use polars::prelude::*;
use std::collections::BTreeMap;

/// columns for transactions
#[derive(Default, cryo_to_df::ToDataFrames)]
pub struct FourByteCounts {
    pub(crate) n_rows: u64,
    pub(crate) block_number: Vec<Option<u32>>,
    pub(crate) transaction_index: Vec<Option<u32>>,
    pub(crate) transaction_hash: Vec<Option<RawBytes>>,
    pub(crate) signature: Vec<RawBytes>,
    pub(crate) size: Vec<u64>,
    pub(crate) count: Vec<u64>,
    pub(crate) chain_id: Vec<u64>,
}

#[async_trait::async_trait]
impl Dataset for FourByteCounts {
    fn aliases() -> Vec<&'static str> {
        vec!["4byte_counts"]
    }
}

type BlockTxsTraces = (Option<u32>, Vec<Option<RawBytes>>, Vec<BTreeMap<String, u64>>);

#[async_trait::async_trait]
impl CollectByBlock for FourByteCounts {
    type Response = BlockTxsTraces;

    async fn extract(request: Params, source: Arc<Source>, query: Arc<Query>) -> R<Self::Response> {
        let schema =
            query.schemas.get(&Datatype::FourByteCounts).ok_or(err("schema not provided"))?;
        let include_txs = schema.has_column("transaction_hash");
        source
            .geth_debug_trace_block_4byte_traces(request.block_number()? as u32, include_txs)
            .await
    }

    fn transform(response: Self::Response, columns: &mut Self, query: &Arc<Query>) -> R<()> {
        process_storage_reads(&response, columns, &query.schemas)
    }
}

#[async_trait::async_trait]
impl CollectByTransaction for FourByteCounts {
    type Response = BlockTxsTraces;

    async fn extract(request: Params, source: Arc<Source>, query: Arc<Query>) -> R<Self::Response> {
        let schema =
            query.schemas.get(&Datatype::FourByteCounts).ok_or(err("schema not provided"))?;
        let include_block_number = schema.has_column("block_number");
        let tx = request.transaction_hash()?;
        source.geth_debug_trace_transaction_4byte_traces(tx, include_block_number).await
    }

    fn transform(response: Self::Response, columns: &mut Self, query: &Arc<Query>) -> R<()> {
        process_storage_reads(&response, columns, &query.schemas)
    }
}

pub(crate) fn process_storage_reads(
    response: &BlockTxsTraces,
    columns: &mut FourByteCounts,
    schemas: &Schemas,
) -> R<()> {
    let schema = schemas.get(&Datatype::FourByteCounts).ok_or(err("schema not provided"))?;
    let (block_number, txs, traces) = response;
    for (index, (trace, tx)) in traces.iter().zip(txs).enumerate() {
        for (signature_size, count) in trace.iter() {
            let (signature, size) = parse_signature_size(signature_size)?;
            columns.n_rows += 1;
            store!(schema, columns, block_number, *block_number);
            store!(schema, columns, transaction_index, Some(index as u32));
            store!(schema, columns, transaction_hash, tx.clone());
            store!(schema, columns, signature, signature.clone());
            store!(schema, columns, size, size);
            store!(schema, columns, count, *count);
        }
    }
    Ok(())
}

fn parse_signature_size(signature_size: &str) -> R<(RawBytes, u64)> {
    let parts: Vec<&str> = signature_size.splitn(2, '-').collect();
    if parts.len() != 2 {
        return Err(err("could not parse 4byte-size pair"))
    }

    // Parse the hexadecimal part (assuming there's no "0x" prefix as in the example given)
    let hex_part = parts[0].trim_start_matches("0x");
    let bytes = (0..hex_part.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&hex_part[i..i + 2], 16))
        .collect::<Result<RawBytes, _>>()
        .map_err(|_| err("could not parse signature bytes"))?;

    // Parse the number as u64
    let number = parts[1].parse::<u64>().map_err(|_| err("could not parse call data size"))?;

    Ok((bytes, number))
}
