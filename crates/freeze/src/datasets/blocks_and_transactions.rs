use crate::Datatype;
use crate::*;
use std::collections::HashMap;
use polars::prelude::*;
use crate::types::collection::*;

type Result<T> = ::core::result::Result<T, CollectError>;

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

// impl BlocksAndTransactions {
// }

    // fn create_dfs(self, schemas: &Schemas, chain_id: u64) -> Result<HashMap<Datatype, DataFrame>>;

    // fn create_dfs(self, schemas: &Schemas, chain_id: u64) -> Result<HashMap<Datatype, DataFrame>> {
    //     let BlocksAndTransactions(block_columns, transaction_columns) = self;
    //     Ok(vec![
    //         (Datatype::Blocks, block_columns.create_df(schemas, chain_id)?),
    //         (Datatype::Transactions, transaction_columns.create_df(schemas, chain_id)?),
    //     ]
    //     .into_iter()
    //     .collect())
    // }

// // impl MultiColumnData for BlocksAndTransactions {
// //     fn datatypes() -> Vec<Datatype> {
// //         vec![Datatype::Blocks, Datatype::Transactions]
// //     }
// // }
// type Result<T> = ::core::result::Result<T, CollectError>;

// #[async_trait::async_trait]
// pub trait MultiCollectByBlock {
//     /// type of block data responses
//     type Response;

//     /// fetch dataset data by block
//     async fn extract(request: Params, source: Source, schemas: Schemas) -> Result<Self::Response>;

//     /// transform block data response into column data
//     fn transform(response: Self::Response, columns: &mut Self, schemas: &Schemas);

//     /// collect data into DataFrame
//     async fn collect_by_block(
//         partition: Partition,
//         source: Source,
//         schemas: &HashMap<Datatype, Table>,
//     ) -> Result<HashMap<Datatype, DataFrame>> {
//         let (sender, receiver) = mpsc::channel(1);
//         let chain_id = source.chain_id;
//         fetch_partition(
//             Self::extract,
//             partition,
//             source,
//             schemas.clone(),
//             Self::block_parameters(),
//             sender,
//         )
//         .await?;
//         Self::block_data_to_dfs(receiver, schemas, chain_id).await
//     }

//     /// convert block-derived data to dataframe
//     async fn block_data_to_dfs(
//         mut receiver: mpsc::Receiver<Result<Self::Response>>,
//         schemas: &HashMap<Datatype, Table>,
//         chain_id: u64,
//     ) -> Result<HashMap<Datatype, DataFrame>> {
//         let mut columns = Self::default();
//         while let Some(message) = receiver.recv().await {
//             match message {
//                 Ok(message) => Self::transform(message, &mut columns, schemas),
//                 Err(e) => return Err(e),
//             }
//         }
//         columns.create_dfs(schemas, chain_id)
//     }

// }


// #[async_trait::async_trait]
// impl MultiCollectByBlock for BlocksAndTransactions {
//     type Response = <Transactions as CollectByBlock>::Response;

//     async fn extract(request: Params, source: Source, schemas: Schemas) -> Result<Self::Response> {
//         <Transactions as CollectByBlock>::extract(request, source, schemas).await
//     }

//     fn transform(response: Self::Response, columns: &mut Self, schemas: &Schemas) {
//         let BlocksAndTransactions(blocks, transactions) = columns;
//         let (block, _) = response.clone();
//         super::blocks::process_block(
//             block,
//             blocks,
//             schemas.get(&Datatype::Blocks).expect("schema undefined"),
//         );
//         <Transactions as CollectByBlock>::transform(response, transactions, schemas);
//     }

// }
