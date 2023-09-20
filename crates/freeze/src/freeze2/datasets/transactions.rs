use crate::{
    conversions::{ToVecHex, ToVecU8},
    dataframes::SortableDataFrame,
    freeze2::{ChunkDim, CollectByBlock, CollectByTransaction, ColumnData, RpcParams},
    store, with_series, with_series_binary, with_series_u256, CollectError, ColumnEncoding,
    ColumnType, Source, Table, Transactions, U256Type,
};
use ethers::prelude::*;
use polars::prelude::*;

#[async_trait::async_trait]
impl CollectByBlock for Transactions {
    type BlockResponse = (Block<Transaction>, Option<Vec<u32>>);

    type BlockColumns = TransactionColumns;

    fn block_parameters() -> Vec<ChunkDim> {
        vec![ChunkDim::BlockNumber]
    }

    async fn extract_by_block(
        request: RpcParams,
        source: Source,
        schema: Table,
    ) -> Result<Self::BlockResponse, CollectError> {
        let block = source
            .fetcher
            .get_block_with_txs(request.block_number())
            .await?
            .ok_or(CollectError::CollectError("block not found".to_string()))?;
        let gas_used = if schema.has_column("gas_used") {
            Some(source.get_txs_gas_used(&block).await?)
        } else {
            None
        };
        Ok((block, gas_used))
    }

    fn transform_by_block(
        response: Self::BlockResponse,
        columns: &mut Self::BlockColumns,
        schema: &Table,
    ) {
        let (block, gas_used) = response;
        match gas_used {
            Some(gas_used) => {
                for (tx, gas_used) in block.transactions.into_iter().zip(gas_used.iter()) {
                    process_transaction(tx, Some(*gas_used), columns, schema);
                }
            }
            None => {
                for tx in block.transactions.into_iter() {
                    process_transaction(tx, None, columns, schema);
                }
            }
        }
    }
}

#[async_trait::async_trait]
impl CollectByTransaction for Transactions {
    type TransactionResponse = (Transaction, Option<u32>);

    type TransactionColumns = TransactionColumns;

    fn transaction_parameters() -> Vec<ChunkDim> {
        vec![ChunkDim::Transaction]
    }

    async fn extract_by_transaction(
        request: RpcParams,
        source: Source,
        schema: Table,
    ) -> Result<Self::TransactionResponse, CollectError> {
        let tx_hash = H256::from_slice(&request.transaction());
        let transaction = source
            .fetcher
            .get_transaction(tx_hash)
            .await?
            .ok_or(CollectError::CollectError("transaction not found".to_string()))?;
        let gas_used = if schema.has_column("gas_used") {
            source
                .fetcher
                .get_transaction_receipt(tx_hash)
                .await?
                .ok_or(CollectError::CollectError("transaction not found".to_string()))?
                .gas_used
                .map(|x| x.as_u32())
        } else {
            None
        };
        Ok((transaction, gas_used))
    }

    fn transform_by_transaction(
        response: Self::TransactionResponse,
        columns: &mut Self::TransactionColumns,
        schema: &Table,
    ) {
        let (transaction, gas_used) = response;
        process_transaction(transaction, gas_used, columns, schema);
    }
}

/// columns for transactions
#[cryo_to_df::to_df]
#[derive(Default)]
pub struct TransactionColumns {
    n_rows: u64,
    block_number: Vec<Option<u64>>,
    transaction_index: Vec<Option<u64>>,
    transaction_hash: Vec<Vec<u8>>,
    nonce: Vec<u64>,
    from_address: Vec<Vec<u8>>,
    to_address: Vec<Option<Vec<u8>>>,
    value: Vec<U256>,
    input: Vec<Vec<u8>>,
    gas_limit: Vec<u32>,
    gas_used: Vec<u32>,
    gas_price: Vec<Option<u64>>,
    transaction_type: Vec<Option<u32>>,
    max_priority_fee_per_gas: Vec<Option<u64>>,
    max_fee_per_gas: Vec<Option<u64>>,
}

fn process_transaction(
    tx: Transaction,
    gas_used: Option<u32>,
    columns: &mut TransactionColumns,
    schema: &Table,
) {
    columns.n_rows += 1;
    store!(schema, columns, block_number, tx.block_number.map(|x| x.as_u64()));
    store!(schema, columns, transaction_index, tx.transaction_index.map(|x| x.as_u64()));
    store!(schema, columns, transaction_hash, tx.hash.as_bytes().to_vec());
    store!(schema, columns, from_address, tx.from.as_bytes().to_vec());
    store!(schema, columns, to_address, tx.to.map(|x| x.as_bytes().to_vec()));
    store!(schema, columns, nonce, tx.nonce.as_u64());
    store!(schema, columns, value, tx.value);
    store!(schema, columns, input, tx.input.to_vec());
    store!(schema, columns, gas_limit, tx.gas.as_u32());
    store!(schema, columns, gas_used, gas_used.unwrap());
    store!(schema, columns, gas_price, tx.gas_price.map(|gas_price| gas_price.as_u64()));
    store!(schema, columns, transaction_type, tx.transaction_type.map(|value| value.as_u32()));
    store!(schema, columns, max_fee_per_gas, tx.max_fee_per_gas.map(|value| value.as_u64()));
    store!(
        schema,
        columns,
        max_priority_fee_per_gas,
        tx.max_priority_fee_per_gas.map(|value| value.as_u64())
    );
}
