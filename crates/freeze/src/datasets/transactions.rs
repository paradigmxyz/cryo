use crate::*;
use ethers::prelude::*;
use polars::prelude::*;

/// columns for transactions
#[cryo_to_df::to_df(Datatype::Transactions)]
#[derive(Default)]
pub struct Transactions {
    n_rows: u64,
    block_number: Vec<Option<u32>>,
    transaction_index: Vec<Option<u64>>,
    transaction_hash: Vec<Vec<u8>>,
    nonce: Vec<u64>,
    from_address: Vec<Vec<u8>>,
    to_address: Vec<Option<Vec<u8>>>,
    value: Vec<U256>,
    input: Vec<Vec<u8>>,
    gas_limit: Vec<u64>,
    gas_used: Vec<Option<u64>>,
    gas_price: Vec<Option<u64>>,
    transaction_type: Vec<Option<u32>>,
    max_priority_fee_per_gas: Vec<Option<u64>>,
    max_fee_per_gas: Vec<Option<u64>>,
    success: Vec<bool>,
    chain_id: Vec<u64>,
}

#[async_trait::async_trait]
impl Dataset for Transactions {
    fn aliases() -> Vec<&'static str> {
        vec!["txs"]
    }
}

#[async_trait::async_trait]
impl CollectByBlock for Transactions {
    type Response = (Block<Transaction>, Option<Vec<TransactionReceipt>>, bool);

    async fn extract(request: Params, source: Arc<Source>, query: Arc<Query>) -> R<Self::Response> {
        let block = source
            .fetcher
            .get_block_with_txs(request.block_number()?)
            .await?
            .ok_or(CollectError::CollectError("block not found".to_string()))?;
        let schema = query.schemas.get_schema(&Datatype::Transactions)?;
        let receipt = if schema.has_column("gas_used") | schema.has_column("success") {
            Some(source.fetcher.get_block_receipts(request.block_number()?).await?)
        } else {
            None
        };
        Ok((block, receipt, query.exclude_failed))
    }

    fn transform(response: Self::Response, columns: &mut Self, query: &Arc<Query>) -> R<()> {
        let schema = query.schemas.get_schema(&Datatype::Transactions)?;
        let (block, receipts, exclude_failed) = response;
        match receipts {
            Some(receipts) => {
                for (tx, receipt) in block.transactions.into_iter().zip(receipts) {
                    process_transaction(
                        tx,
                        Some(receipt.clone()),
                        columns,
                        schema,
                        exclude_failed,
                    )?;
                }
            }
            None => {
                for tx in block.transactions.into_iter() {
                    process_transaction(tx, None, columns, schema, exclude_failed)?;
                }
            }
        }
        Ok(())
    }
}

#[async_trait::async_trait]
impl CollectByTransaction for Transactions {
    type Response = (Transaction, Option<TransactionReceipt>, bool);

    async fn extract(request: Params, source: Arc<Source>, query: Arc<Query>) -> R<Self::Response> {
        let tx_hash = request.ethers_transaction_hash()?;
        let schema = query.schemas.get_schema(&Datatype::Transactions)?;
        let transaction = source
            .fetcher
            .get_transaction(tx_hash)
            .await?
            .ok_or(CollectError::CollectError("transaction not found".to_string()))?;
        let gas_used = if schema.has_column("gas_used") {
            source.fetcher.get_transaction_receipt(tx_hash).await?
        } else {
            None
        };
        Ok((transaction, gas_used, query.exclude_failed))
    }

    fn transform(response: Self::Response, columns: &mut Self, query: &Arc<Query>) -> R<()> {
        let schema = query.schemas.get_schema(&Datatype::Transactions)?;
        let (transaction, receipt, exclude_failed) = response;
        process_transaction(transaction, receipt, columns, schema, exclude_failed)?;
        Ok(())
    }
}

pub(crate) fn process_transaction(
    tx: Transaction,
    receipt: Option<TransactionReceipt>,
    columns: &mut Transactions,
    schema: &Table,
    exclude_failed: bool,
) -> R<()> {
    let success = if exclude_failed | schema.has_column("success") {
        let success = tx_success(&tx, &receipt)?;
        if exclude_failed & !success {
            return Ok(())
        }
        success
    } else {
        false
    };

    columns.n_rows += 1;
    store!(schema, columns, block_number, tx.block_number.map(|x| x.as_u32()));
    store!(schema, columns, transaction_index, tx.transaction_index.map(|x| x.as_u64()));
    store!(schema, columns, transaction_hash, tx.hash.as_bytes().to_vec());
    store!(schema, columns, from_address, tx.from.as_bytes().to_vec());
    store!(schema, columns, to_address, tx.to.map(|x| x.as_bytes().to_vec()));
    store!(schema, columns, nonce, tx.nonce.as_u64());
    store!(schema, columns, value, tx.value);
    store!(schema, columns, input, tx.input.to_vec());
    store!(schema, columns, gas_limit, tx.gas.as_u64());
    store!(schema, columns, success, success);
    store!(schema, columns, gas_used, receipt.and_then(|r| r.gas_used.map(|x| x.as_u64())));
    store!(schema, columns, gas_price, tx.gas_price.map(|gas_price| gas_price.as_u64()));
    store!(schema, columns, transaction_type, tx.transaction_type.map(|value| value.as_u32()));
    store!(schema, columns, max_fee_per_gas, tx.max_fee_per_gas.map(|value| value.as_u64()));
    store!(
        schema,
        columns,
        max_priority_fee_per_gas,
        tx.max_priority_fee_per_gas.map(|value| value.as_u64())
    );

    Ok(())
}

fn tx_success(tx: &Transaction, receipt: &Option<TransactionReceipt>) -> R<bool> {
    if let Some(status) = receipt.as_ref().and_then(|x| x.status) {
        Ok(status.as_u64() == 1)
    } else if let (Some(1), Some(true)) =
        (tx.chain_id.map(|x| x.as_u64()), tx.block_number.map(|x| x.as_u64() < 4370000))
    {
        if let Some(gas_used) = receipt.as_ref().and_then(|x| x.gas_used.map(|x| x.as_u64())) {
            Ok(gas_used == 0)
        } else {
            return Err(err("could not determine status of transaction"))
        }
    } else {
        return Err(err("could not determine status of transaction"))
    }
}
