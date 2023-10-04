use crate::Datatype;
use crate::*;
use std::collections::HashMap;
use polars::prelude::*;
use crate::types::collection::*;

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

    fn transform(response: Self::Response, columns: &mut Self, schemas: &Schemas) {
        let BlocksAndTransactions(blocks, transactions) = columns;
        let (block, _) = response.clone();
        super::blocks::process_block(
            block,
            blocks,
            schemas.get(&Datatype::Blocks).expect("schema undefined"),
        );
        <Transactions as CollectByBlock>::transform(response, transactions, schemas);
    }
}
