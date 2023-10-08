use crate::{
    err, types::chunks::Subchunk, AddressChunk, BlockChunk, CallDataChunk, ChunkData, ChunkStats,
    CollectError, Params, SlotChunk, TopicChunk, TransactionChunk,
};

/// a dimension of chunking
#[derive(Debug, Clone, Copy, Eq, PartialEq, Hash, serde::Serialize)]
pub enum Dim {
    /// Block number dimension
    BlockNumber,
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
    pub fn plural_name(&self) -> &str {
        match self {
            Dim::BlockNumber => "blocks",
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

impl std::str::FromStr for Dim {
    type Err = crate::ParseError;

    /// convert str to Dim
    fn from_str(name: &str) -> Result<Dim, Self::Err> {
        let dim = match name {
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
            _ => return Err(crate::ParseError::ParseError("invalid dim name".to_string())),
        };
        Ok(dim)
    }
}

impl std::fmt::Display for Dim {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let as_str = match self {
            Dim::BlockNumber => "block",
            Dim::TransactionHash => "transaction",
            Dim::CallData => "call_data",
            Dim::Address => "address",
            Dim::Contract => "contract",
            Dim::ToAddress => "to_address",
            Dim::Slot => "slot",
            Dim::Topic0 => "topic0",
            Dim::Topic1 => "topic1",
            Dim::Topic2 => "topic2",
            Dim::Topic3 => "topic3",
        };
        write!(f, "{}", as_str)
    }
}

/// a group of chunks along multiple dimensions
#[derive(Clone, Default, Debug)]
pub struct Partition {
    /// label
    pub label: Option<Vec<Option<String>>>,
    /// block numbers
    pub block_numbers: Option<Vec<BlockChunk>>,
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
    ($outputs:expr, $key:ident) => {{
        let partitions: Result<Vec<_>, CollectError> = $outputs
            .iter()
            .flat_map(|output| {
                output
                    .$key
                    .as_ref()
                    .ok_or(CollectError::CollectError("error".to_string()))
                    .into_iter()
                    .flatten()
                    .map(|chunk| {
                        Ok(Partition { $key: Some(vec![chunk.clone()]), ..output.clone() })
                    })
            })
            .collect();
        partitions
    }};
}

/// parametrize outputs
macro_rules! parametrize {
    ($outputs:expr, $new_outputs:expr, $self_chunks:expr, $param_key:ident) => {
        for output in $outputs.into_iter() {
            let chunks = $self_chunks
                .as_ref()
                .ok_or(CollectError::CollectError("mising block ranges".to_string()))?;

            for chunk in chunks.iter() {
                for value in chunk.values().iter() {
                    $new_outputs.push(Params { $param_key: Some(value.clone()), ..output.clone() })
                }
            }
        }
    };
}

/// label partition
macro_rules! label_partition {
    ($outputs:expr, $dim_labels:expr, $key:ident) => {{
        $outputs
            .iter()
            .flat_map(|output| {
                let chunks = match output.$key.as_ref() {
                    Some(chunks) => chunks,
                    None => {
                        return vec![Err(CollectError::CollectError(
                            "missing entries for partition dimension".to_string(),
                        ))]
                        .into_iter()
                    }
                };
                let dls = match &$dim_labels {
                    Some(dls2) => {
                        if chunks.len() != dls2.len() {
                            return vec![Err(CollectError::CollectError(
                                "number of chunks should equal number of labels for dim"
                                    .to_string(),
                            ))]
                            .into_iter()
                        }
                        dls2.clone()
                    }
                    None => {
                        vec![None; chunks.len()]
                    }
                };
                let partitions: Vec<_> = chunks
                    .iter()
                    .zip(dls.into_iter())
                    .map(move |(chunk, label)| {
                        let lbl = output
                            .label
                            .clone()
                            .ok_or(CollectError::CollectError("missing label".to_string()))?;
                        Ok(Partition {
                            label: Some([lbl, vec![label.clone()]].concat()),
                            $key: Some(vec![chunk.clone()]),
                            ..output.clone()
                        })
                    })
                    .collect();
                partitions.into_iter()
            })
            .collect::<Result<Vec<_>, _>>()
    }};
}

fn chunks_to_name<T: ChunkData>(chunks: &Option<Vec<T>>) -> Result<String, CollectError> {
    chunks
        .as_ref()
        .ok_or(err("partition chunks missing"))?
        .stub()
        .map_err(|_| CollectError::CollectError("could not determine name of chunk".to_string()))
}

impl Partition {
    /// get label of partition
    pub fn label_pieces(&self, partitioned_by: &[Dim]) -> Result<Vec<String>, CollectError> {
        let stored_pieces = self.label.clone().unwrap_or_else(|| vec![None; partitioned_by.len()]);

        if stored_pieces.len() != partitioned_by.len() {
            return Err(CollectError::CollectError(
                "self.label length must match number of partition dimensions".to_string(),
            ))
        }

        let mut pieces = Vec::new();
        for (dim, piece) in partitioned_by.iter().zip(stored_pieces.iter()) {
            let piece = match piece.clone() {
                Some(x) => x,
                None => match dim {
                    Dim::BlockNumber => chunks_to_name(&self.block_numbers)?,
                    Dim::TransactionHash => chunks_to_name(&self.transactions)?,
                    Dim::CallData => chunks_to_name(&self.call_datas)?,
                    Dim::Address => chunks_to_name(&self.addresses)?,
                    Dim::Contract => chunks_to_name(&self.contracts)?,
                    Dim::ToAddress => chunks_to_name(&self.to_addresses)?,
                    Dim::Slot => chunks_to_name(&self.slots)?,
                    Dim::Topic0 => chunks_to_name(&self.topic0s)?,
                    Dim::Topic1 => chunks_to_name(&self.topic1s)?,
                    Dim::Topic2 => chunks_to_name(&self.topic2s)?,
                    Dim::Topic3 => chunks_to_name(&self.topic3s)?,
                },
            };
            pieces.push(piece);
        }
        Ok(pieces)
    }

    /// get label of partition
    pub fn label(&self, partitioned_by: &[Dim]) -> Result<String, CollectError> {
        Ok(self.label_pieces(partitioned_by)?.join("__"))
    }

    /// partition Partition along given partition dimensions
    pub fn partition(&self, partition_by: Vec<Dim>) -> Result<Vec<Partition>, CollectError> {
        let mut outputs = vec![self.clone()];
        for chunk_dimension in partition_by.iter() {
            outputs = match chunk_dimension {
                Dim::BlockNumber => partition!(outputs, block_numbers)?,
                Dim::TransactionHash => partition!(outputs, transactions)?,
                Dim::Address => partition!(outputs, addresses)?,
                Dim::Contract => partition!(outputs, contracts)?,
                Dim::ToAddress => partition!(outputs, to_addresses)?,
                Dim::CallData => partition!(outputs, call_datas)?,
                Dim::Slot => partition!(outputs, slots)?,
                Dim::Topic0 => partition!(outputs, topic0s)?,
                Dim::Topic1 => partition!(outputs, topic1s)?,
                Dim::Topic2 => partition!(outputs, topic2s)?,
                Dim::Topic3 => partition!(outputs, topic3s)?,
            }
        }
        Ok(outputs)
    }

    /// partition while respecting labels, ignoring all labels currently in partition
    /// each non-None entry in labels should have same length as number of self dim chunks
    pub fn partition_with_labels(
        &self,
        labels: PartitionLabels,
        partition_by: Vec<Dim>,
    ) -> Result<Vec<Partition>, CollectError> {
        let mut outputs = vec![Partition { label: Some(Vec::new()), ..self.clone() }];
        for chunk_dimension in partition_by.iter() {
            let dim_labels = labels.dim(chunk_dimension);
            outputs = match chunk_dimension {
                Dim::BlockNumber => label_partition!(outputs, dim_labels, block_numbers)?,
                Dim::TransactionHash => label_partition!(outputs, dim_labels, transactions)?,
                Dim::Address => label_partition!(outputs, dim_labels, addresses)?,
                Dim::Contract => label_partition!(outputs, dim_labels, contracts)?,
                Dim::ToAddress => label_partition!(outputs, dim_labels, to_addresses)?,
                Dim::CallData => label_partition!(outputs, dim_labels, call_datas)?,
                Dim::Slot => label_partition!(outputs, dim_labels, slots)?,
                Dim::Topic0 => label_partition!(outputs, dim_labels, topic0s)?,
                Dim::Topic1 => label_partition!(outputs, dim_labels, topic1s)?,
                Dim::Topic2 => label_partition!(outputs, dim_labels, topic2s)?,
                Dim::Topic3 => label_partition!(outputs, dim_labels, topic3s)?,
            }
        }
        Ok(outputs)
    }

    /// iterate through param sets of Partition
    pub fn param_sets(&self, inner_request_size: Option<u64>) -> Result<Vec<Params>, CollectError> {
        let dims = self.dims();
        let include_block_ranges = inner_request_size.is_some() && dims.contains(&Dim::BlockNumber);

        let mut outputs = vec![Params::default()];
        for dimension in self.dims().iter() {
            let mut new = Vec::new();
            match dimension {
                Dim::BlockNumber => {
                    if !include_block_ranges {
                        parametrize!(outputs, new, self.block_numbers, block_number)
                    } else {
                        new = outputs
                    }
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
            }
            outputs = new;
        }

        // partition blocks by inner request size
        let outputs = match (inner_request_size, self.block_numbers.clone(), include_block_ranges) {
            (_, _, false) => outputs,
            (Some(inner_request_size), Some(block_numbers), true) => {
                let mut block_ranges = Vec::new();
                for block_chunk in block_numbers.subchunk_by_size(&inner_request_size) {
                    match block_chunk {
                        BlockChunk::Range(start, end) => block_ranges.push(Some((start, end))),
                        BlockChunk::Numbers(values) => {
                            for value in values.iter() {
                                block_ranges.push(Some((*value, *value)))
                            }
                        }
                    }
                }

                let mut new_outputs = Vec::new();
                for output in outputs.iter() {
                    for block_range in block_ranges.iter() {
                        new_outputs.push(Params { block_range: *block_range, ..output.clone() })
                    }
                }
                new_outputs
            }
            _ => {
                return Err(CollectError::CollectError(
                    "insufficient block information present in partition".to_string(),
                ))
            }
        };

        Ok(outputs)
    }

    /// return Vec of dimensions defined in partitions
    pub fn dims(&self) -> Vec<Dim> {
        let mut dims = Vec::new();
        if self.block_numbers.is_some() {
            dims.push(Dim::BlockNumber)
        };
        if self.transactions.is_some() {
            dims.push(Dim::TransactionHash)
        };
        if self.addresses.is_some() {
            dims.push(Dim::Address)
        };
        if self.contracts.is_some() {
            dims.push(Dim::Contract)
        };
        if self.to_addresses.is_some() {
            dims.push(Dim::ToAddress)
        };
        if self.call_datas.is_some() {
            dims.push(Dim::CallData)
        };
        if self.slots.is_some() {
            dims.push(Dim::Slot)
        };
        if self.topic0s.is_some() {
            dims.push(Dim::Topic0)
        };
        if self.topic1s.is_some() {
            dims.push(Dim::Topic1)
        };
        if self.topic2s.is_some() {
            dims.push(Dim::Topic2)
        };
        if self.topic3s.is_some() {
            dims.push(Dim::Topic3)
        };
        dims
    }

    /// number of chunks for a particular dimension
    pub fn n_chunks(&self, dim: &Dim) -> usize {
        match dim {
            Dim::BlockNumber => self.block_numbers.as_ref().map(|x| x.len()).unwrap_or(0),
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
