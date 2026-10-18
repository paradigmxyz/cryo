use crate::*;
use alloy::{
    consensus::Transaction as ConsensusTransaction,
    primitives::{Address, TxKind, U256},
    rpc::types::{
        Block, BlockTransactions, BlockTransactionsKind, Transaction, TransactionReceipt,
    },
};
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
    transaction_type: Vec<u32>,
    max_priority_fee_per_gas: Vec<Option<u64>>,
    max_fee_per_gas: Vec<Option<u64>>,
    success: Vec<bool>,
    n_input_bytes: Vec<u32>,
    n_input_zero_bytes: Vec<u32>,
    n_input_nonzero_bytes: Vec<u32>,
    n_rlp_bytes: Vec<u32>,
    block_hash: Vec<Vec<u8>>,
    chain_id: Vec<u64>,
    timestamp: Vec<u32>,
    r: Vec<Vec<u8>>,
    s: Vec<Vec<u8>>,
    v: Vec<bool>,
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
    type Response = (Block, Vec<TransactionAndReceipt>, bool);

    async fn extract(request: Params, source: Arc<Source>, query: Arc<Query>) -> R<Self::Response> {
        let block = source
            .get_block(request.block_number()?, BlockTransactionsKind::Full)
            .await?
            .ok_or(CollectError::CollectError("block not found".to_string()))?;
        let schema = query.schemas.get_schema(&Datatype::Transactions)?;

        // 1. collect transactions and filter them if optional parameters are supplied
        // filter by from_address
        let from_filter: Box<dyn Fn(&Transaction) -> bool + Send> =
            if let Some(from_address) = &request.from_address {
                Box::new(move |tx| tx.inner.signer() == Address::from_slice(from_address))
            } else {
                Box::new(|_| true)
            };
        // filter by to_address
        let to_filter: Box<dyn Fn(&Transaction) -> bool + Send> =
            if let Some(to_address) = &request.to_address {
                Box::new(move |tx| match tx.inner.kind() {
                    TxKind::Create => false,
                    TxKind::Call(address) => address == Address::from_slice(to_address),
                })
            } else {
                Box::new(|_| true)
            };
        let transactions: Vec<Transaction> = block
            .transactions
            .clone()
            .as_transactions()
            .unwrap()
            .iter()
            .filter(|&x| from_filter(x))
            .filter(|&x| to_filter(x))
            .cloned()
            .collect();

        // 2. collect receipts if necessary
        // if transactions are filtered fetch by set of transaction hashes, else fetch all receipts
        // in block
        let receipts: Vec<Option<_>> =
            if schema.has_column("gas_used") | schema.has_column("success") {
                // receipts required
                let receipts = if request.from_address.is_some() || request.to_address.is_some() {
                    source.get_tx_receipts(BlockTransactions::Full(transactions.clone())).await?
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
            let gas_price = get_gas_price(&block, &tx);
            process_transaction(
                tx,
                receipt,
                columns,
                schema,
                exclude_failed,
                block.header.timestamp as u32,
                gas_price,
            )?;
        }
        Ok(())
    }
}

#[async_trait::async_trait]
impl CollectByTransaction for Transactions {
    type Response = (TransactionAndReceipt, Block, bool, u32);

    async fn extract(request: Params, source: Arc<Source>, query: Arc<Query>) -> R<Self::Response> {
        let tx_hash = request.ethers_transaction_hash()?;
        let schema = query.schemas.get_schema(&Datatype::Transactions)?;
        let transaction = source
            .get_transaction_by_hash(tx_hash)
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
            .get_block(block_number, BlockTransactionsKind::Hashes)
            .await?
            .ok_or(CollectError::CollectError("block not found".to_string()))?;

        let timestamp = block.header.timestamp as u32;

        Ok(((transaction, receipt), block, query.exclude_failed, timestamp))
    }

    fn transform(response: Self::Response, columns: &mut Self, query: &Arc<Query>) -> R<()> {
        let schema = query.schemas.get_schema(&Datatype::Transactions)?;
        let ((transaction, receipt), block, exclude_failed, timestamp) = response;
        let gas_price = get_gas_price(&block, &transaction);
        process_transaction(
            transaction,
            receipt,
            columns,
            schema,
            exclude_failed,
            timestamp,
            gas_price,
        )?;
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
    gas_price: Option<u64>,
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
    store!(schema, columns, block_number, tx.block_number.map(|x| x as u32));
    store!(schema, columns, transaction_index, tx.transaction_index);
    store!(schema, columns, transaction_hash, tx.inner.tx_hash().to_vec());
    store!(schema, columns, from_address, tx.inner.signer().to_vec());
    store!(
        schema,
        columns,
        to_address,
        match tx.inner.kind() {
            TxKind::Create => None,
            TxKind::Call(address) => Some(address.to_vec()),
        }
    );
    store!(schema, columns, nonce, tx.inner.nonce());
    store!(schema, columns, value, tx.inner.value());
    store!(schema, columns, input, tx.inner.input().to_vec());
    store!(schema, columns, gas_limit, tx.inner.gas_limit());
    store!(schema, columns, success, success);
    if schema.has_column("n_input_bytes") |
        schema.has_column("n_input_zero_bytes") |
        schema.has_column("n_input_nonzero_bytes")
    {
        let n_input_bytes = tx.inner.input().len() as u32;
        let n_input_zero_bytes = tx.inner.input().iter().filter(|&&x| x == 0).count() as u32;
        store!(schema, columns, n_input_bytes, n_input_bytes);
        store!(schema, columns, n_input_zero_bytes, n_input_zero_bytes);
        store!(schema, columns, n_input_nonzero_bytes, n_input_bytes - n_input_zero_bytes);
    }
    // in alloy eip2718_encoded_length is rlp_encoded_length
    store!(schema, columns, n_rlp_bytes, tx.inner.eip2718_encoded_length() as u32);
    store!(schema, columns, gas_used, receipt.as_ref().map(|r| r.gas_used));
    // store!(schema, columns, gas_price, Some(receipt.unwrap().effective_gas_price as u64));
    store!(schema, columns, gas_price, gas_price);
    store!(schema, columns, transaction_type, tx.inner.tx_type() as u32);
    store!(schema, columns, max_fee_per_gas, get_max_fee_per_gas(&tx));
    store!(
        schema,
        columns,
        max_priority_fee_per_gas,
        tx.inner.max_priority_fee_per_gas().map(|value| value as u64)
    );
    store!(schema, columns, timestamp, timestamp);
    store!(schema, columns, block_hash, tx.block_hash.unwrap_or_default().to_vec());

    store!(schema, columns, v, tx.inner.signature().v());
    store!(schema, columns, r, tx.inner.signature().r().to_vec_u8());
    store!(schema, columns, s, tx.inner.signature().s().to_vec_u8());

    Ok(())
}

fn get_max_fee_per_gas(tx: &Transaction) -> Option<u64> {
    match tx.inner.as_ref() {
        alloy::consensus::TxEnvelope::Legacy(_) => None,
        alloy::consensus::TxEnvelope::Eip2930(_) => None,
        _ => Some(tx.inner.max_fee_per_gas() as u64),
    }
}

pub(crate) fn get_gas_price(block: &Block, tx: &Transaction) -> Option<u64> {
    match tx.inner.as_ref() {
        alloy::consensus::TxEnvelope::Legacy(_) => tx.gas_price().map(|gas_price| gas_price as u64),
        alloy::consensus::TxEnvelope::Eip2930(_) => {
            tx.gas_price().map(|gas_price| gas_price as u64)
        }
        _ => {
            let base_fee_per_gas = block.header.inner.base_fee_per_gas.unwrap();
            let priority_fee = std::cmp::min(
                tx.inner.max_priority_fee_per_gas().unwrap() as u64,
                tx.inner.max_fee_per_gas() as u64 - base_fee_per_gas,
            );
            Some(base_fee_per_gas + priority_fee)
        }
    }
}

fn tx_success(tx: &Transaction, receipt: &Option<TransactionReceipt>) -> R<bool> {
    if let Some(r) = receipt {
        Ok(r.status())
    } else if let (Some(1), Some(true)) =
        (tx.inner.chain_id(), tx.block_number.map(|x| x < 4370000))
    {
        if let Some(r) = receipt {
            Ok(r.gas_used == 0)
        } else {
            Err(err("could not determine status of transaction"))
        }
    } else {
        Err(err("could not determine status of transaction"))
    }
}
