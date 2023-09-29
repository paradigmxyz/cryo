// use std::collections::HashMap;

// use crate::types::{Datatype, MultiDatatype, Source, Table};
// use crate::{
//     Blocks, BlocksAndTransactions, CollectError, Logs, MetaChunk, MetaDatatype, Traces,
//     Transactions, CollectByBlock, CollectByTransaction
// };
// use polars::prelude::*;

// const TX_ERROR: String = "datatype cannot collect by transaction".to_string();

// pub async fn collect_by_block(
//     datatype: MetaDatatype,
//     meta_chunk: MetaChunk,
//     source: Source,
//     schemas: HashMap<Datatype, Table>,
// ) -> Result<HashMap<Datatype, DataFrame>, CollectError> {
//     let task = match datatype {
//         MetaDatatype::Scalar(datatype) => match datatype {
//             Datatype::Blocks => Blocks::collect_by_block(meta_chunk, source, schemas),
//             Datatype::Logs => Logs::collect_by_block(meta_chunk, source, schemas),
//             Datatype::Traces => Traces::collect_by_block(meta_chunk, source, schemas),
//             Datatype::Transactions => Transactions::collect_by_block(meta_chunk, source, schemas),
//             _ => panic!(),
//         },
//         MetaDatatype::Multi(datatype) => match datatype {
//             MultiDatatype::BlocksAndTransactions => {
//                 BlocksAndTransactions::collect_by_block(meta_chunk, source, schemas)
//             }
//             MultiDatatype::StateDiffs => {
//                 panic!();
//                 // StateDiffs::collect_by_block(meta_chunk, source, schema),
//             }
//         },
//     };
//     task.await
// }

// pub async fn collect_by_transaction(
//     datatype: MetaDatatype,
//     meta_chunk: MetaChunk,
//     source: Source,
//     schemas: HashMap<Datatype, Table>,
// ) -> Result<HashMap<Datatype, DataFrame>, CollectError> {
//     let task = match datatype {
//         MetaDatatype::Scalar(datatype) => match datatype {
//             Datatype::Blocks => Blocks::collect_by_transaction(meta_chunk, source, schemas),
//             Datatype::Logs => Logs::collect_by_transaction(meta_chunk, source, schemas),
//             Datatype::Traces => Traces::collect_by_transaction(meta_chunk, source, schemas),
//             Datatype::Transactions => {
//                 Transactions::collect_by_transaction(meta_chunk, source, schemas)
//             }
//             _ => panic!(),
//         },
//         MetaDatatype::Multi(datatype) => match datatype {
//             MultiDatatype::BlocksAndTransactions => Err(CollectError::CollectError(TX_ERROR))?,
//             MultiDatatype::StateDiffs => Err(CollectError::CollectError(TX_ERROR))?,
//         },
//     };
//     task.await
// }
