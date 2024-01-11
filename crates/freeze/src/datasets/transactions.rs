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
    n_input_bytes: Vec<u32>,
    n_input_zero_bytes: Vec<u32>,
    n_input_nonzero_bytes: Vec<u32>,
    block_hash: Vec<Vec<u8>>,
    chain_id: Vec<u64>,
    timestamp: Vec<u32>,
}

#[async_trait::async_trait]
impl Dataset for Transactions {
    fn aliases() -> Vec<&'static str> {
        vec!["txs"]
    }

    fn default_columns() -> Option<Vec<&'static str>> {
        Some(vec![
            "block_number",
            "transaction_index",
            "transaction_hash",
            "nonce",
            "from_address",
            "to_address",
            "value",
            "input",
            "gas_limit",
            "gas_used",
            "gas_price",
            "transaction_type",
            "max_priority_fee_per_gas",
            "max_fee_per_gas",
            "success",
            "n_input_bytes",
            "n_input_zero_bytes",
            "n_input_nonzero_bytes",
            "chain_id",
        ])
    }

    fn optional_parameters() -> Vec<Dim> {
        vec![Dim::FromAddress, Dim::ToAddress]
    }
}

/// tuple representing transaction and optional receipt
pub type TransactionAndReceipt = (Transaction, Option<TransactionReceipt>);

#[async_trait::async_trait]
impl CollectByBlock for Transactions {
    type Response = (Block<Transaction>, Vec<TransactionAndReceipt>, bool);

    async fn extract(request: Params, source: Arc<Source>, query: Arc<Query>) -> R<Self::Response> {
        let block = source
            .get_block_with_txs(request.block_number()?)
            .await?
            .ok_or(CollectError::CollectError("block not found".to_string()))?;
        let schema = query.schemas.get_schema(&Datatype::Transactions)?;

        // 1. collect transactions and filter them if optional parameters are supplied
        // filter by from_address
        let from_filter: Box<dyn Fn(&Transaction) -> bool + Send> =
            if let Some(from_address) = &request.from_address {
                Box::new(move |tx| tx.from.as_bytes() == from_address)
            } else {
                Box::new(|_| true)
            };
        // filter by to_address
        let to_filter: Box<dyn Fn(&Transaction) -> bool + Send> =
            if let Some(to_address) = &request.to_address {
                Box::new(move |tx| tx.to.as_ref().map_or(false, |x| x.as_bytes() == to_address))
            } else {
                Box::new(|_| true)
            };
        let transactions =
            block.transactions.clone().into_iter().filter(from_filter).filter(to_filter).collect();

        // 2. collect receipts if necessary
        // if transactions are filtered fetch by set of transaction hashes, else fetch all receipts
        // in block
        let receipts: Vec<Option<_>> =
            if schema.has_column("gas_used") | schema.has_column("success") {
                // receipts required
                let receipts = if request.from_address.is_some() || request.to_address.is_some() {
                    source.get_tx_receipts(&transactions).await?
                } else {
                    source.get_tx_receipts_in_block(&block).await?
                };
                receipts.into_iter().map(Some).collect()
            } else {
                vec![None; block.transactions.len()]
            };

        let transactions_with_receips = transactions.into_iter().zip(receipts).collect();
        Ok((block, transactions_with_receips, query.exclude_failed))
    }

    fn transform(response: Self::Response, columns: &mut Self, query: &Arc<Query>) -> R<()> {
        let schema = query.schemas.get_schema(&Datatype::Transactions)?;
        let (block, transactions_with_receipts, exclude_failed) = response;
        for (tx, receipt) in transactions_with_receipts.into_iter() {
            process_transaction(
                tx,
                receipt,
                columns,
                schema,
                exclude_failed,
                block.timestamp.as_u32(),
            )?;
        }
        Ok(())
    }
}

#[async_trait::async_trait]
impl CollectByTransaction for Transactions {
    type Response = (TransactionAndReceipt, bool, u32);

    async fn extract(request: Params, source: Arc<Source>, query: Arc<Query>) -> R<Self::Response> {
        let tx_hash = request.ethers_transaction_hash()?;
        let schema = query.schemas.get_schema(&Datatype::Transactions)?;
        let transaction = source
            .get_transaction(tx_hash)
            .await?
            .ok_or(CollectError::CollectError("transaction not found".to_string()))?;
        let receipt = if schema.has_column("gas_used") {
            source.get_transaction_receipt(tx_hash).await?
        } else {
            None
        };

        let block_number = transaction
            .block_number
            .ok_or(CollectError::CollectError("no block number for tx".to_string()))?;

        let block = source
            .get_block(block_number.as_u64())
            .await?
            .ok_or(CollectError::CollectError("block not found".to_string()))?;

        let timestamp = block.timestamp.as_u32();

        Ok(((transaction, receipt), query.exclude_failed, timestamp))
    }

    fn transform(response: Self::Response, columns: &mut Self, query: &Arc<Query>) -> R<()> {
        let schema = query.schemas.get_schema(&Datatype::Transactions)?;
        let ((transaction, receipt), exclude_failed, timestamp) = response;
        process_transaction(transaction, receipt, columns, schema, exclude_failed, timestamp)?;
        Ok(())
    }
}

pub(crate) fn process_transaction(
    tx: Transaction,
    receipt: Option<TransactionReceipt>,
    columns: &mut Transactions,
    schema: &Table,
    exclude_failed: bool,
    timestamp: u32,
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
    if schema.has_column("n_input_bytes") |
        schema.has_column("n_input_zero_bytes") |
        schema.has_column("n_input_nonzero_bytes")
    {
        let n_input_bytes = tx.input.len() as u32;
        let n_input_zero_bytes = tx.input.iter().filter(|&&x| x == 0).count() as u32;
        store!(schema, columns, n_input_bytes, n_input_bytes);
        store!(schema, columns, n_input_zero_bytes, n_input_zero_bytes);
        store!(schema, columns, n_input_nonzero_bytes, n_input_bytes - n_input_zero_bytes);
    }
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
    store!(schema, columns, timestamp, timestamp);
    store!(schema, columns, block_hash, tx.block_hash.unwrap_or_default().as_bytes().to_vec());

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
