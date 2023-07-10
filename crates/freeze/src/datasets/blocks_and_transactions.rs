use std::collections::HashMap;
use std::collections::HashSet;

use polars::prelude::*;

use crate::types::BlockChunk;
use crate::types::BlocksAndTransactions;
use crate::types::CollectError;
use crate::types::Datatype;
use crate::types::FreezeOpts;
use crate::types::MultiDataset;

#[async_trait::async_trait]
impl MultiDataset for BlocksAndTransactions {
    fn datatypes(&self) -> HashSet<Datatype> {
        [
            Datatype::Blocks,
            Datatype::Transactions,
        ]
        .into_iter()
        .collect()
    }

    async fn collect_chunk(
        &self,
        _block_chunk: &BlockChunk,
        _opts: &FreezeOpts,
    ) -> Result<HashMap<Datatype, DataFrame>, CollectError> {
        panic!()
    }
}
