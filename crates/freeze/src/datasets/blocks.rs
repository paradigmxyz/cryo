use crate::*;
use alloy::{
    primitives::U256,
    rpc::types::{Block, BlockTransactionsKind},
};
use polars::prelude::*;

/// columns for transactions
#[derive(Default, cryo_to_df::ToDataFrames)]
pub struct Blocks {
    n_rows: u64,
    block_hash: Vec<Option<RawBytes>>,
    parent_hash: Vec<RawBytes>,
    uncles_hash: Vec<RawBytes>,
    author: Vec<Option<RawBytes>>,
    state_root: Vec<RawBytes>,
    transactions_root: Vec<RawBytes>,
    receipts_root: Vec<RawBytes>,
    block_number: Vec<Option<u32>>,
    gas_used: Vec<u64>,
    gas_limit: Vec<u64>,
    extra_data: Vec<RawBytes>,
    logs_bloom: Vec<Option<RawBytes>>,
    timestamp: Vec<u32>,
    difficulty: Vec<u64>,
    total_difficulty: Vec<Option<U256>>,
    size: Vec<Option<u64>>,
    mix_hash: Vec<Option<RawBytes>>,
    nonce: Vec<Option<RawBytes>>,
    base_fee_per_gas: Vec<Option<u64>>,
    withdrawals_root: Vec<Option<RawBytes>>,
    chain_id: Vec<u64>,
}

#[async_trait::async_trait]
impl Dataset for Blocks {
    fn default_columns() -> Option<Vec<&'static str>> {
        Some(vec![
            "block_number",
            "block_hash",
            "timestamp",
            "author",
            "gas_used",
            "extra_data",
            "base_fee_per_gas",
            "chain_id",
        ])
    }
}

#[async_trait::async_trait]
impl CollectByBlock for Blocks {
    type Response = Block;

    async fn extract(request: Params, source: Arc<Source>, _: Arc<Query>) -> R<Self::Response> {
        let block = source
            .get_block(request.block_number()?, BlockTransactionsKind::Hashes)
            .await?
            .ok_or(CollectError::CollectError("block not found".to_string()))?;
        Ok(block)
    }

    fn transform(response: Self::Response, columns: &mut Self, query: &Arc<Query>) -> R<()> {
        let schema = query.schemas.get_schema(&Datatype::Blocks)?;
        process_block(response, columns, schema)
    }
}

#[async_trait::async_trait]
impl CollectByTransaction for Blocks {
    type Response = Block;

    async fn extract(request: Params, source: Arc<Source>, _: Arc<Query>) -> R<Self::Response> {
        let transaction = source
            .get_transaction_by_hash(request.ethers_transaction_hash()?)
            .await?
            .ok_or(CollectError::CollectError("transaction not found".to_string()))?;
        let block = source
            .get_block_by_hash(
                transaction.block_hash.ok_or(err("no block block_hash found"))?,
                BlockTransactionsKind::Hashes,
            )
            .await?
            .ok_or(CollectError::CollectError("block not found".to_string()))?;
        Ok(block)
    }

    fn transform(response: Self::Response, columns: &mut Self, query: &Arc<Query>) -> R<()> {
        let schema = query.schemas.get_schema(&Datatype::Blocks)?;
        process_block(response, columns, schema)
    }
}

/// process block into columns
pub(crate) fn process_block<TX>(block: Block<TX>, columns: &mut Blocks, schema: &Table) -> R<()> {
    columns.n_rows += 1;

    store!(schema, columns, block_hash, Some(block.header.hash.to_vec()));
    store!(schema, columns, parent_hash, block.header.parent_hash.0.to_vec());
    store!(
        schema,
        columns,
        uncles_hash,
        block.uncles.into_iter().flat_map(|s| s.to_vec()).collect()
    );
    store!(schema, columns, author, Some(block.header.beneficiary.to_vec()));
    store!(schema, columns, state_root, block.header.state_root.0.to_vec());
    store!(schema, columns, transactions_root, block.header.transactions_root.0.to_vec());
    store!(schema, columns, receipts_root, block.header.receipts_root.0.to_vec());
    store!(schema, columns, block_number, Some(block.header.number as u32));
    store!(schema, columns, gas_used, block.header.gas_used);
    store!(schema, columns, gas_limit, block.header.gas_limit);
    store!(schema, columns, extra_data, block.header.extra_data.to_vec());
    store!(schema, columns, logs_bloom, Some(block.header.logs_bloom.to_vec()));
    store!(schema, columns, timestamp, block.header.timestamp as u32);
    store!(schema, columns, difficulty, block.header.difficulty.wrapping_to::<u64>());
    store!(schema, columns, total_difficulty, block.header.total_difficulty);
    store!(schema, columns, base_fee_per_gas, block.header.base_fee_per_gas);
    store!(schema, columns, size, block.header.size.map(|v| v.wrapping_to::<u64>()));
    store!(schema, columns, mix_hash, Some(block.header.mix_hash.to_vec()));
    store!(schema, columns, nonce, Some(block.header.nonce.0.to_vec()));
    store!(schema, columns, withdrawals_root, block.header.withdrawals_root.map(|x| x.0.to_vec()));
    Ok(())
}
