use crate::{
    AddressChunk, BlockChunk, CallDataChunk, ChunkData, ChunkStats, CollectError, Params,
    SlotChunk, TopicChunk, TransactionChunk,
};

/// a dimension of chunking
#[derive(Debug, Clone, Copy, Eq, PartialEq, serde::Serialize)]
pub enum Dim {
    /// Block number dimension
    BlockNumber,
    /// Block range dimension
    BlockRange,
    /// Transaction hash dimension
    TransactionHash,
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

impl Dim {
    /// list of all dimensions
    pub fn all_dims() -> Vec<Dim> {
        vec![
            Dim::BlockNumber,
            Dim::BlockRange,
            Dim::TransactionHash,
            Dim::CallData,
            Dim::Address,
            Dim::Contract,
            Dim::ToAddress,
            Dim::Slot,
            Dim::Topic0,
            Dim::Topic1,
            Dim::Topic2,
            Dim::Topic3,
        ]
    }

    /// convert str to Dim
    pub fn from_name(name: String) -> Dim {
        match name.as_str() {
            "block" => Dim::BlockNumber,
            "transaction" => Dim::TransactionHash,
            "call_data" => Dim::CallData,
            "address" => Dim::Address,
            "contract" => Dim::Contract,
            "to_address" => Dim::ToAddress,
            "slot" => Dim::Slot,
            "topic0" => Dim::Topic0,
            "topic1" => Dim::Topic1,
            "topic2" => Dim::Topic2,
            "topic3" => Dim::Topic3,
            _ => panic!("unknown dimension name"),
        }
    }

    /// convert str to Dim
    pub fn plural_name(&self) -> &str {
        match self {
            Dim::BlockNumber => "blocks",
            Dim::BlockRange => "blocks",
            Dim::TransactionHash => "transactions",
            Dim::CallData => "call_datas",
            Dim::Address => "addresses",
            Dim::Contract => "contracts",
            Dim::ToAddress => "to_addresses",
            Dim::Slot => "slots",
            Dim::Topic0 => "topic0s",
            Dim::Topic1 => "topic1s",
            Dim::Topic2 => "topic2s",
            Dim::Topic3 => "topic3s",
        }
    }
}

/// a group of chunks along multiple dimensions
#[derive(Clone, Default, Debug)]
pub struct Partition {
    /// label
    pub label: Option<Vec<Option<String>>>,
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
                    .map(|chunk| Partition { $key: Some(vec![chunk.clone()]), ..output.clone() })
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
                    $new_outputs.push(Params { $param_key: Some(value.clone()), ..output.clone() })
                }
            }
        }
    };
}

/// label partition
macro_rules! label_partition {
    ($outputs:expr, $dim_labels:expr, $key:ident) => {
        $outputs
            .iter()
            .flat_map(|output| {
                let chunks = output.$key.as_ref().expect("missing entries for partition dimension");
                let dls = match &$dim_labels {
                    Some(dls2) => {
                        if chunks.len() != dls2.len() {
                            panic!("number of chunks should equal number of labels for dim")
                        }
                        dls2.clone()
                    }
                    None => {
                        vec![None; chunks.len()]
                    }
                };
                chunks.iter().zip(dls.into_iter()).map(|(chunk, label)| Partition {
                    label: Some([output.label.clone().unwrap(), vec![label.clone()]].concat()),
                    $key: Some(vec![chunk.clone()]),
                    ..output.clone()
                })
            })
            .collect()
    };
}

fn chunks_to_name<T: ChunkData>(chunks: &Option<Vec<T>>) -> String {
    chunks
        .as_ref()
        .expect("partition chunks missing")
        .stub()
        .map_err(|_| CollectError::CollectError("could not determine name of chunk".to_string()))
        .unwrap()
}

impl Partition {
    /// get label of partition
    pub fn label_pieces(&self, partitioned_by: &[Dim]) -> Vec<String> {
        let stored_pieces = self.label.clone().unwrap_or_else(|| vec![None; partitioned_by.len()]);

        if stored_pieces.len() != partitioned_by.len() {
            panic!("self.label length must match number of partition dimensions");
        }

        let mut pieces = Vec::new();
        for (dim, piece) in partitioned_by.iter().zip(stored_pieces.iter()) {
            let piece = piece.clone().unwrap_or_else(|| match dim {
                Dim::BlockNumber => chunks_to_name(&self.block_numbers),
                Dim::TransactionHash => chunks_to_name(&self.transactions),
                Dim::BlockRange => chunks_to_name(&self.block_ranges),
                Dim::CallData => chunks_to_name(&self.call_datas),
                Dim::Address => chunks_to_name(&self.addresses),
                Dim::Contract => chunks_to_name(&self.contracts),
                Dim::ToAddress => chunks_to_name(&self.to_addresses),
                Dim::Slot => chunks_to_name(&self.slots),
                Dim::Topic0 => chunks_to_name(&self.topic0s),
                Dim::Topic1 => chunks_to_name(&self.topic1s),
                Dim::Topic2 => chunks_to_name(&self.topic2s),
                Dim::Topic3 => chunks_to_name(&self.topic3s),
            });
            pieces.push(piece);
        }
        pieces
    }

    /// get label of partition
    pub fn label(&self, partitioned_by: &[Dim]) -> String {
        self.label_pieces(partitioned_by).join("__")
    }

    /// partition Partition along given partition dimensions
    pub fn partition(&self, partition_by: Vec<Dim>) -> Vec<Partition> {
        let mut outputs = vec![self.clone()];
        for chunk_dimension in partition_by.iter() {
            outputs = match chunk_dimension {
                Dim::BlockNumber => partition!(outputs, block_numbers),
                Dim::BlockRange => partition!(outputs, block_ranges),
                Dim::TransactionHash => partition!(outputs, transactions),
                Dim::Address => partition!(outputs, addresses),
                Dim::Contract => partition!(outputs, contracts),
                Dim::ToAddress => partition!(outputs, to_addresses),
                Dim::CallData => partition!(outputs, call_datas),
                Dim::Slot => partition!(outputs, slots),
                Dim::Topic0 => partition!(outputs, topic0s),
                Dim::Topic1 => partition!(outputs, topic1s),
                Dim::Topic2 => partition!(outputs, topic2s),
                Dim::Topic3 => partition!(outputs, topic3s),
            }
        }
        outputs
    }

    /// partition while respecting labels, ignoring all labels currently in partition
    /// each non-None entry in labels should have same length as number of self dim chunks
    pub fn partition_with_labels(
        &self,
        labels: PartitionLabels,
        partition_by: Vec<Dim>,
    ) -> Vec<Partition> {
        let mut outputs = vec![Partition { label: Some(Vec::new()), ..self.clone() }];
        for chunk_dimension in partition_by.iter() {
            let dim_labels = labels.dim(chunk_dimension);
            outputs = match chunk_dimension {
                Dim::BlockNumber => label_partition!(outputs, dim_labels, block_numbers),
                Dim::BlockRange => label_partition!(outputs, dim_labels, block_ranges),
                Dim::TransactionHash => label_partition!(outputs, dim_labels, transactions),
                Dim::Address => label_partition!(outputs, dim_labels, addresses),
                Dim::Contract => label_partition!(outputs, dim_labels, contracts),
                Dim::ToAddress => label_partition!(outputs, dim_labels, to_addresses),
                Dim::CallData => label_partition!(outputs, dim_labels, call_datas),
                Dim::Slot => label_partition!(outputs, dim_labels, slots),
                Dim::Topic0 => label_partition!(outputs, dim_labels, topic0s),
                Dim::Topic1 => label_partition!(outputs, dim_labels, topic1s),
                Dim::Topic2 => label_partition!(outputs, dim_labels, topic2s),
                Dim::Topic3 => label_partition!(outputs, dim_labels, topic3s),
            }
        }
        outputs
    }

    /// iterate through param sets of Partition
    pub fn param_sets(&self, dimensions: Vec<Dim>) -> Vec<Params> {
        let mut outputs = vec![Params::default()];
        for dimension in dimensions.iter() {
            let mut new = Vec::new();
            match dimension {
                Dim::BlockNumber => {
                    parametrize!(outputs, new, self.block_numbers, block_number)
                }
                Dim::TransactionHash => {
                    parametrize!(outputs, new, self.transactions, transaction_hash)
                }
                Dim::Address => parametrize!(outputs, new, self.addresses, address),
                Dim::Contract => parametrize!(outputs, new, self.contracts, contract),
                Dim::ToAddress => parametrize!(outputs, new, self.to_addresses, to_address),
                Dim::CallData => parametrize!(outputs, new, self.call_datas, call_data),
                Dim::Slot => parametrize!(outputs, new, self.slots, slot),
                Dim::Topic0 => parametrize!(outputs, new, self.topic0s, topic0),
                Dim::Topic1 => parametrize!(outputs, new, self.topic1s, topic1),
                Dim::Topic2 => parametrize!(outputs, new, self.topic2s, topic2),
                Dim::Topic3 => parametrize!(outputs, new, self.topic3s, topic3),
                Dim::BlockRange => {
                    for output in outputs.into_iter() {
                        for chunk in self.block_ranges.as_ref().unwrap().iter() {
                            match chunk {
                                BlockChunk::Range(start, end) => new.push(Params {
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

    /// number of chunks for a particular dimension
    pub fn n_chunks(&self, dim: &Dim) -> usize {
        match dim {
            Dim::BlockNumber => self.block_numbers.as_ref().map(|x| x.len()).unwrap_or(0),
            Dim::BlockRange => self.block_ranges.as_ref().map(|x| x.len()).unwrap_or(0),
            Dim::TransactionHash => self.transactions.as_ref().map(|x| x.len()).unwrap_or(0),
            Dim::Address => self.addresses.as_ref().map(|x| x.len()).unwrap_or(0),
            Dim::Contract => self.contracts.as_ref().map(|x| x.len()).unwrap_or(0),
            Dim::ToAddress => self.to_addresses.as_ref().map(|x| x.len()).unwrap_or(0),
            Dim::CallData => self.call_datas.as_ref().map(|x| x.len()).unwrap_or(0),
            Dim::Slot => self.slots.as_ref().map(|x| x.len()).unwrap_or(0),
            Dim::Topic0 => self.topic0s.as_ref().map(|x| x.len()).unwrap_or(0),
            Dim::Topic1 => self.topic1s.as_ref().map(|x| x.len()).unwrap_or(0),
            Dim::Topic2 => self.topic2s.as_ref().map(|x| x.len()).unwrap_or(0),
            Dim::Topic3 => self.topic3s.as_ref().map(|x| x.len()).unwrap_or(0),
        }
    }

    /// get statistics for partition
    pub fn stats(&self) -> PartitionStats {
        let chunk = self.clone();
        PartitionStats {
            block_numbers: chunk.block_numbers.map(|c| c.stats()),
            block_ranges: chunk.block_ranges.map(|c| c.stats()),
            transactions: chunk.transactions.map(|c| c.stats()),
            call_datas: chunk.call_datas.map(|c| c.stats()),
            addresses: chunk.addresses.map(|c| c.stats()),
            contracts: chunk.contracts.map(|c| c.stats()),
            to_addresses: chunk.to_addresses.map(|c| c.stats()),
            slots: chunk.slots.map(|c| c.stats()),
            topic0s: chunk.topic0s.map(|c| c.stats()),
            topic1s: chunk.topic1s.map(|c| c.stats()),
            topic2s: chunk.topic2s.map(|c| c.stats()),
            topic3s: chunk.topic3s.map(|c| c.stats()),
        }
    }
}

/// compute stats for Vec of Partition's
pub fn meta_chunks_stats(chunks: &[Partition]) -> PartitionStats {
    chunks
        .iter()
        .map(|chunk| chunk.stats())
        .fold(PartitionStats { ..Default::default() }, |acc, stats| acc.fold(stats))
}

/// stats of Partition
#[derive(Default)]
pub struct PartitionStats {
    /// block numbers stats
    pub block_numbers: Option<ChunkStats<u64>>,
    /// block ranges stats
    pub block_ranges: Option<ChunkStats<u64>>,
    /// transactions stats
    pub transactions: Option<ChunkStats<Vec<u8>>>,
    /// call datas stats
    pub call_datas: Option<ChunkStats<Vec<u8>>>,
    /// addresses stats
    pub addresses: Option<ChunkStats<Vec<u8>>>,
    /// contracts stats
    pub contracts: Option<ChunkStats<Vec<u8>>>,
    /// to_addresses stats
    pub to_addresses: Option<ChunkStats<Vec<u8>>>,
    /// slots stats
    pub slots: Option<ChunkStats<Vec<u8>>>,
    /// topic0s stats
    pub topic0s: Option<ChunkStats<Vec<u8>>>,
    /// topic1s stats
    pub topic1s: Option<ChunkStats<Vec<u8>>>,
    /// topic2s stats
    pub topic2s: Option<ChunkStats<Vec<u8>>>,
    /// topic3s stats
    pub topic3s: Option<ChunkStats<Vec<u8>>>,
}

fn fold<T: std::cmp::Ord + crate::types::chunks::chunk_ops::ValueToString>(
    lhs: Option<ChunkStats<T>>,
    rhs: Option<ChunkStats<T>>,
) -> Option<ChunkStats<T>> {
    match (lhs, rhs) {
        (Some(lhs), Some(rhs)) => Some(lhs.fold(rhs)),
        (Some(lhs), None) => Some(lhs),
        (None, Some(rhs)) => Some(rhs),
        (None, None) => None,
    }
}

impl PartitionStats {
    fn fold(self, other: PartitionStats) -> PartitionStats {
        PartitionStats {
            block_numbers: fold(self.block_numbers, other.block_numbers),
            block_ranges: fold(self.block_ranges, other.block_ranges),
            transactions: fold(self.transactions, other.transactions),
            call_datas: fold(self.call_datas, other.call_datas),
            addresses: fold(self.addresses, other.addresses),
            contracts: fold(self.contracts, other.contracts),
            to_addresses: fold(self.to_addresses, other.to_addresses),
            slots: fold(self.slots, other.slots),
            topic0s: fold(self.topic0s, other.topic0s),
            topic1s: fold(self.topic1s, other.topic1s),
            topic2s: fold(self.topic2s, other.topic2s),
            topic3s: fold(self.topic3s, other.topic3s),
        }
    }
}

/// labels for Partition
pub struct PartitionLabels {
    /// block number labels
    pub block_number_labels: Option<Vec<Option<String>>>,
    /// block range labels
    pub block_range_labels: Option<Vec<Option<String>>>,
    /// transaction hash labels
    pub transaction_hash_labels: Option<Vec<Option<String>>>,
    /// call data labels
    pub call_data_labels: Option<Vec<Option<String>>>,
    /// address labels
    pub address_labels: Option<Vec<Option<String>>>,
    /// contract labels
    pub contract_labels: Option<Vec<Option<String>>>,
    /// to address labels
    pub to_address_labels: Option<Vec<Option<String>>>,
    /// slot labels
    pub slot_labels: Option<Vec<Option<String>>>,
    /// topic0 labels
    pub topic0_labels: Option<Vec<Option<String>>>,
    /// topic1 labels
    pub topic1_labels: Option<Vec<Option<String>>>,
    /// topic2 labels
    pub topic2_labels: Option<Vec<Option<String>>>,
    /// topic3 labels
    pub topic3_labels: Option<Vec<Option<String>>>,
}

impl PartitionLabels {
    fn dim(&self, dim: &Dim) -> Option<Vec<Option<String>>> {
        match dim {
            Dim::BlockNumber => self.block_number_labels.clone(),
            Dim::BlockRange => self.block_range_labels.clone(),
            Dim::TransactionHash => self.transaction_hash_labels.clone(),
            Dim::CallData => self.call_data_labels.clone(),
            Dim::Address => self.address_labels.clone(),
            Dim::Contract => self.contract_labels.clone(),
            Dim::ToAddress => self.to_address_labels.clone(),
            Dim::Slot => self.slot_labels.clone(),
            Dim::Topic0 => self.topic0_labels.clone(),
            Dim::Topic1 => self.topic1_labels.clone(),
            Dim::Topic2 => self.topic2_labels.clone(),
            Dim::Topic3 => self.topic3_labels.clone(),
        }
    }

    /// whether dimension is labeled
    pub fn dim_labeled(&self, dim: &Dim) -> bool {
        match self.dim(dim) {
            None => false,
            Some(labels) => labels.iter().any(|label| label.is_some()),
        }
    }
}
