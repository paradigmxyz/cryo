use crate::*;
use ethers::prelude::*;
use polars::prelude::*;

/// columns for transactions
#[cryo_to_df::to_df(Datatype::Blocks)]
#[derive(Default)]
pub struct Blocks {
    n_rows: u64,
    block_hash: Vec<Option<Vec<u8>>>,
    parent_hash: Vec<Vec<u8>>,
    author: Vec<Option<Vec<u8>>>,
    state_root: Vec<Vec<u8>>,
    transactions_root: Vec<Vec<u8>>,
    receipts_root: Vec<Vec<u8>>,
    block_number: Vec<Option<u32>>,
    gas_used: Vec<u64>,
    extra_data: Vec<Vec<u8>>,
    logs_bloom: Vec<Option<Vec<u8>>>,
    timestamp: Vec<u32>,
    total_difficulty: Vec<Option<U256>>,
    size: Vec<Option<u32>>,
    base_fee_per_gas: Vec<Option<u64>>,
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
    type Response = Block<TxHash>;

    async fn extract(request: Params, source: Arc<Source>, _: Arc<Query>) -> R<Self::Response> {
        let block = source
            .fetcher
            .get_block(request.block_number()?)
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
    type Response = Block<TxHash>;

    async fn extract(request: Params, source: Arc<Source>, _: Arc<Query>) -> R<Self::Response> {
        let transaction = source
            .fetcher
            .get_transaction(request.ethers_transaction_hash()?)
            .await?
            .ok_or(CollectError::CollectError("transaction not found".to_string()))?;
        let block = source
            .fetcher
            .get_block_by_hash(transaction.block_hash.ok_or(err("no block block_hash found"))?)
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

    store!(schema, columns, block_hash, block.hash.map(|x| x.0.to_vec()));
    store!(schema, columns, parent_hash, block.parent_hash.0.to_vec());
    store!(schema, columns, author, block.author.map(|x| x.0.to_vec()));
    store!(schema, columns, state_root, block.state_root.0.to_vec());
    store!(schema, columns, transactions_root, block.transactions_root.0.to_vec());
    store!(schema, columns, receipts_root, block.receipts_root.0.to_vec());
    store!(schema, columns, block_number, block.number.map(|x| x.as_u32()));
    store!(schema, columns, gas_used, block.gas_used.as_u64());
    store!(schema, columns, extra_data, block.extra_data.to_vec());
    store!(schema, columns, logs_bloom, block.logs_bloom.map(|x| x.0.to_vec()));
    store!(schema, columns, timestamp, block.timestamp.as_u32());
    store!(schema, columns, total_difficulty, block.total_difficulty);
    store!(schema, columns, base_fee_per_gas, block.base_fee_per_gas.map(|x| x.as_u64()));
    store!(schema, columns, size, block.size.map(|x| x.as_u32()));
    Ok(())
}
