use crate::*;
use ethers::prelude::*;
use polars::prelude::*;
use std::collections::HashMap;

/// columns for transactions
#[cryo_to_df::to_df(Datatype::Transactions)]
#[derive(Default)]
pub struct Transactions {
    n_rows: u64,
    block_number: Vec<Option<u64>>,
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
    chain_id: Vec<u64>,
}

#[async_trait::async_trait]
impl Dataset for Transactions {
    fn name() -> &'static str {
        "transactions"
    }

    fn aliases() -> Vec<&'static str> {
        vec!["txs"]
    }

    fn default_sort() -> Vec<String> {
        vec!["block_number".to_string(), "transaction_index".to_string()]
    }
}

type Result<T> = ::core::result::Result<T, CollectError>;

#[async_trait::async_trait]
impl CollectByBlock for Transactions {
    type Response = (Block<Transaction>, Option<Vec<u64>>);

    async fn extract(
        request: Params,
        source: Arc<Source>,
        schemas: Schemas,
    ) -> Result<Self::Response> {
        let block = source
            .fetcher
            .get_block_with_txs(request.block_number()?)
            .await?
            .ok_or(CollectError::CollectError("block not found".to_string()))?;
        let schema = schemas.get(&Datatype::Transactions).ok_or(err("schema not provided"))?;
        let gas_used = if schema.has_column("gas_used") {
            Some(source.get_txs_gas_used(&block).await?)
        } else {
            None
        };
        Ok((block, gas_used))
    }

    fn transform(response: Self::Response, columns: &mut Self, schemas: &Schemas) -> Result<()> {
        let schema = schemas.get(&Datatype::Transactions).ok_or(err("schema not provided"))?;
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
        Ok(())
    }
}

#[async_trait::async_trait]
impl CollectByTransaction for Transactions {
    type Response = (Transaction, Option<u64>);

    async fn extract(
        request: Params,
        source: Arc<Source>,
        schemas: Schemas,
    ) -> Result<Self::Response> {
        let tx_hash = request.ethers_transaction_hash()?;
        let schema = schemas.get(&Datatype::Transactions).ok_or(err("schema not provided"))?;
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
                .map(|x| x.as_u64())
        } else {
            None
        };
        Ok((transaction, gas_used))
    }

    fn transform(response: Self::Response, columns: &mut Self, schemas: &Schemas) -> Result<()> {
        let (transaction, gas_used) = response;
        let schema = schemas.get(&Datatype::Transactions).ok_or(err("schema not provided"))?;
        process_transaction(transaction, gas_used, columns, schema);
        Ok(())
    }
}

pub(crate) fn process_transaction(
    tx: Transaction,
    gas_used: Option<u64>,
    columns: &mut Transactions,
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
    store!(schema, columns, gas_limit, tx.gas.as_u64());
    store!(schema, columns, gas_used, gas_used);
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
