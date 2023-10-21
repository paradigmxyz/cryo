use crate::{datasets::transactions, types::collection::*, Datatype, *};
use polars::prelude::*;
use std::collections::HashMap;

/// BlocksAndTransactions
#[derive(Default)]
pub struct BlocksAndTransactions(Blocks, Transactions);

impl ToDataFrames for BlocksAndTransactions {
    fn create_dfs(
        self,
        schemas: &HashMap<Datatype, Table>,
        chain_id: u64,
    ) -> R<HashMap<Datatype, DataFrame>> {
        let BlocksAndTransactions(blocks, transactions) = self;
        let mut output = HashMap::new();
        output.extend(blocks.create_dfs(schemas, chain_id)?);
        output.extend(transactions.create_dfs(schemas, chain_id)?);
        Ok(output)
    }
}

#[async_trait::async_trait]
impl CollectByBlock for BlocksAndTransactions {
    type Response = <Transactions as CollectByBlock>::Response;

    async fn extract(request: Params, source: Arc<Source>, query: Arc<Query>) -> R<Self::Response> {
        <Transactions as CollectByBlock>::extract(request, source, query).await
    }

    fn transform(response: Self::Response, columns: &mut Self, query: &Arc<Query>) -> R<()> {
        let BlocksAndTransactions(blocks, transactions) = columns;
        let (block, _, _) = response.clone();
        let schema = query.schemas.get_schema(&Datatype::Blocks)?;
        blocks::process_block(block, blocks, schema)?;
        <Transactions as CollectByBlock>::transform(response, transactions, query)?;
        Ok(())
    }
}

#[async_trait::async_trait]
impl CollectByTransaction for BlocksAndTransactions {
    type Response = (
        <Blocks as CollectByTransaction>::Response,
        <Transactions as CollectByTransaction>::Response,
    );

    async fn extract(request: Params, source: Arc<Source>, query: Arc<Query>) -> R<Self::Response> {
        let (tx, receipt, exclude_failed) =
            <Transactions as CollectByTransaction>::extract(request, source.clone(), query).await?;
        let block_number = tx.block_number.ok_or(err("no block number for tx"))?.as_u64();
        let block = source
            .fetcher
            .get_block(block_number)
            .await?
            .ok_or(CollectError::CollectError("block not found".to_string()))?;
        Ok((block, (tx, receipt, exclude_failed)))
    }

    fn transform(response: Self::Response, columns: &mut Self, query: &Arc<Query>) -> R<()> {
        let BlocksAndTransactions(blocks, transactions) = columns;
        let (block, (tx, receipt, exclude_failed)) = response;
        let schema = query.schemas.get_schema(&Datatype::Blocks)?;
        blocks::process_block(block, blocks, schema)?;
        let schema = query.schemas.get_schema(&Datatype::Transactions)?;
        transactions::process_transaction(tx, receipt, transactions, schema, exclude_failed)?;
        Ok(())
    }
}
