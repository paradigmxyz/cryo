use crate::{types::collection::*, Datatype, *};
use polars::prelude::*;
use std::collections::HashMap;

type Result<T> = ::core::result::Result<T, CollectError>;

/// BlocksAndTransactions
#[derive(Default)]
pub struct BlocksAndTransactions(Blocks, Transactions);

impl ToDataFrames for BlocksAndTransactions {
    fn create_dfs(
        self,
        schemas: &HashMap<Datatype, Table>,
        chain_id: u64,
    ) -> Result<HashMap<Datatype, DataFrame>> {
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

    async fn extract(request: Params, source: Source, schemas: Schemas) -> Result<Self::Response> {
        <Transactions as CollectByBlock>::extract(request, source, schemas).await
    }

    fn transform(response: Self::Response, columns: &mut Self, schemas: &Schemas) -> Result<()> {
        let BlocksAndTransactions(blocks, transactions) = columns;
        let (block, _) = response.clone();
        let schema = schemas.get(&Datatype::Blocks).ok_or(err("schema not provided"))?;
        super::blocks::process_block(block, blocks, schema)?;
        <Transactions as CollectByBlock>::transform(response, transactions, schemas)?;
        Ok(())
    }
}
