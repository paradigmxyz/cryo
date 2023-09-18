use super::{partitions, rpc_params::RpcParams};
/// functions related to partitioning MetaChunks
use crate::{
    AddressChunk, BlockChunk, CallDataChunk, ChunkData, SlotChunk, TopicChunk, TransactionChunk,
};
use crate::{CollectError, Source, Table};
use futures::Future;
use tokio::{sync::mpsc, task};

/// fetch data for a given partition
pub async fn fetch_partition<F, Fut, T>(
    f_request: F,
    meta_chunk: partitions::MetaChunk,
    source: Source,
    schema: Table,
    param_dims: Vec<partitions::ChunkDim>,
    sender: mpsc::Sender<Result<T, CollectError>>,
) -> Result<(), CollectError>
where
    F: Copy
        + Send
        + for<'a> Fn(partitions::RpcParams, Source, Table) -> Fut
        + std::marker::Sync
        + 'static,
    Fut: Future<Output = Result<T, CollectError>> + Send + 'static,
    T: Send + 'static,
{
    let mut handles = Vec::new();
    for rpc_params in meta_chunk.param_sets(param_dims).into_iter() {
        let sender = sender.clone();
        let source = source.clone();
        let schema = schema.clone();
        let handle = task::spawn(async move {
            let result = f_request(rpc_params, source.clone(), schema).await;
            sender.send(result).await.expect("tokio mpsc send failure");
        });
        handles.push(handle);
    }
    Ok(())
}

/// a dimension of chunking
pub enum ChunkDim {
    /// Block number dimension
    BlockNumber,
    /// Block range dimension
    BlockRange,
    /// Transaction dimension
    Transaction,
    /// CallData dimension
    CallData,
    /// Address dimension
    Address,
    /// Contract dimension
    Contract,
    /// ToAddress dimension
    ToAddress,
    /// Slot dimension
    Slot,
    /// Topic0 dimension
    Topic0,
    /// Topic1 dimension
    Topic1,
    /// Topic2 dimension
    Topic2,
    /// Topic3 dimension
    Topic3,
}

/// a group of chunks along multiple dimensions
#[derive(Clone, Default)]
pub struct MetaChunk {
    /// block numbers
    pub block_numbers: Option<Vec<BlockChunk>>,
    /// block ranges
    pub block_ranges: Option<Vec<BlockChunk>>,
    /// transactions
    pub transactions: Option<Vec<TransactionChunk>>,
    /// call datas
    pub call_datas: Option<Vec<CallDataChunk>>,
    /// addresses
    pub addresses: Option<Vec<AddressChunk>>,
    /// contracts
    pub contracts: Option<Vec<AddressChunk>>,
    /// to addresses
    pub to_addresses: Option<Vec<AddressChunk>>,
    /// slots
    pub slots: Option<Vec<SlotChunk>>,
    /// topic0s
    pub topic0s: Option<Vec<TopicChunk>>,
    /// topic1s
    pub topic1s: Option<Vec<TopicChunk>>,
    /// topic2s
    pub topic2s: Option<Vec<TopicChunk>>,
    /// topic3s
    pub topic3s: Option<Vec<TopicChunk>>,
}

/// partition outputs
macro_rules! partition {
    ($outputs:expr, $key:ident) => {
        $outputs
            .iter()
            .flat_map(|output| {
                output
                    .$key
                    .as_ref()
                    .unwrap()
                    .iter()
                    .map(|chunk| MetaChunk { $key: Some(vec![chunk.clone()]), ..output.clone() })
            })
            .collect()
    };
}

/// parametrize outputs
macro_rules! parametrize {
    ($outputs:expr, $new_outputs:expr, $self_chunks:expr, $param_key:ident) => {
        for output in $outputs.into_iter() {
            for chunk in $self_chunks.as_ref().unwrap().iter() {
                for value in chunk.values().iter() {
                    $new_outputs
                        .push(RpcParams { $param_key: Some(value.clone()), ..output.clone() })
                }
            }
        }
    };
}

impl MetaChunk {
    /// partition MetaChunk along given partition dimensions
    pub fn partition_meta_chunk(&self, partition_by: Vec<ChunkDim>) -> Vec<MetaChunk> {
        let mut outputs = vec![self.clone()];
        for chunk_dimension in partition_by.iter() {
            outputs = match chunk_dimension {
                ChunkDim::BlockNumber => partition!(outputs, block_numbers),
                ChunkDim::BlockRange => partition!(outputs, block_ranges),
                ChunkDim::Transaction => partition!(outputs, transactions),
                ChunkDim::Address => partition!(outputs, addresses),
                ChunkDim::Contract => partition!(outputs, contracts),
                ChunkDim::ToAddress => partition!(outputs, to_addresses),
                ChunkDim::CallData => partition!(outputs, call_datas),
                ChunkDim::Slot => partition!(outputs, slots),
                ChunkDim::Topic0 => partition!(outputs, topic0s),
                ChunkDim::Topic1 => partition!(outputs, topic1s),
                ChunkDim::Topic2 => partition!(outputs, topic2s),
                ChunkDim::Topic3 => partition!(outputs, topic3s),
            }
        }
        outputs
    }

    /// iterate through param sets of MetaChunk
    pub fn param_sets(&self, dimensions: Vec<ChunkDim>) -> Vec<RpcParams> {
        let mut outputs = vec![RpcParams::default()];
        for dimension in dimensions.iter() {
            let mut new = Vec::new();
            match dimension {
                ChunkDim::BlockNumber => {
                    parametrize!(outputs, new, self.block_numbers, block_number)
                }
                ChunkDim::Transaction => parametrize!(outputs, new, self.transactions, transaction),
                ChunkDim::Address => parametrize!(outputs, new, self.addresses, address),
                ChunkDim::Contract => parametrize!(outputs, new, self.contracts, contract),
                ChunkDim::ToAddress => parametrize!(outputs, new, self.to_addresses, to_address),
                ChunkDim::CallData => parametrize!(outputs, new, self.call_datas, call_data),
                ChunkDim::Slot => parametrize!(outputs, new, self.slots, slot),
                ChunkDim::Topic0 => parametrize!(outputs, new, self.topic0s, topic0),
                ChunkDim::Topic1 => parametrize!(outputs, new, self.topic1s, topic1),
                ChunkDim::Topic2 => parametrize!(outputs, new, self.topic2s, topic2),
                ChunkDim::Topic3 => parametrize!(outputs, new, self.topic3s, topic3),
                ChunkDim::BlockRange => {
                    for output in outputs.into_iter() {
                        for chunk in self.block_ranges.as_ref().unwrap().iter() {
                            match chunk {
                                BlockChunk::Range(start, end) => new.push(RpcParams {
                                    block_range: Some((*start, *end)),
                                    ..output.clone()
                                }),
                                _ => panic!("not a BlockRange"),
                            }
                        }
                    }
                }
            }
            outputs = new;
        }
        outputs
    }
}
