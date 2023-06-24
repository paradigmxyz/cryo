use ethers::prelude::*;
use ring::digest::{self, Digest};

use crate::types::{BlockChunk, SlimBlock};

pub fn block_to_slim_block<T>(block: &Block<T>) -> SlimBlock {
    SlimBlock {
        number: block.number.unwrap().as_u64(),
        hash: block.hash.unwrap().as_bytes().to_vec(),
        author: block.author.unwrap().as_bytes().to_vec(),
        gas_used: block.gas_used.as_u64(),
        extra_data: block.extra_data.to_vec(),
        timestamp: block.timestamp.as_u64(),
        base_fee_per_gas: block.base_fee_per_gas.map(|value| value.as_u64()),
    }
}

// pub fn get_chunks(opts: &FreezeOpts) -> Vec<BlockChunk> {
//     match opts {
//         FreezeOpts {
//             block_numbers: Some(block_numbers),
//             ..
//         } => block_numbers
//             .chunks(opts.chunk_size as usize)
//             .map(|chunk| BlockChunk {
//                 block_numbers: Some(chunk.to_vec()),
//                 ..Default::default()
//             })
//             .collect(),
//         FreezeOpts {
//             start_block: Some(start_block),
//             end_block: Some(end_block),
//             ..
//         } => {
//             let mut chunks = Vec::new();
//             let mut chunk_start = *start_block;
//             while chunk_start < *end_block {
//                 let chunk_end = (chunk_start + opts.chunk_size).min(*end_block) - 1;
//                 let chunk = BlockChunk {
//                     start_block: Some(chunk_start),
//                     end_block: Some(chunk_end),
//                     ..Default::default()
//                 };
//                 chunks.push(chunk);
//                 chunk_start += opts.chunk_size;
//             }

//             chunks
//         }
//         _ => panic!("invalid block range"),
//     }
// }

pub fn get_subchunks(block_chunk: &BlockChunk, chunk_size: &u64) -> Vec<BlockChunk> {
    match block_chunk {
        BlockChunk {
            block_numbers: Some(block_numbers),
            ..
        } => block_numbers
            .chunks(*chunk_size as usize)
            .map(|chunk| BlockChunk {
                block_numbers: Some(chunk.to_vec()),
                ..Default::default()
            })
            .collect(),
        BlockChunk {
            start_block: Some(start_block),
            end_block: Some(end_block),
            ..
        } => range_to_chunks(start_block, end_block, chunk_size)
            .iter()
            .map(|(start, end)| BlockChunk {
                start_block: Some(*start),
                end_block: Some(*end),
                block_numbers: None,
            })
            .collect(),
        _ => panic!("invalid block range"),
    }
}

/// return a Vec of all blocks in chunk
pub fn get_chunk_block_numbers(block_chunk: &BlockChunk) -> Vec<u64> {
    match block_chunk {
        BlockChunk {
            block_numbers: Some(block_numbers),
            ..
        } => block_numbers.to_vec(),
        BlockChunk {
            start_block: Some(start_block),
            end_block: Some(end_block),
            ..
        } => (*start_block..=*end_block).collect(),
        _ => panic!("invalid block range"),
    }
}

/// break a block chunk into FilterBlockOption for log requests
pub fn block_chunk_to_filter_options(
    block_chunk: &BlockChunk,
    log_request_size: &u64,
) -> Vec<FilterBlockOption> {
    match block_chunk {
        BlockChunk {
            block_numbers: Some(block_numbers),
            ..
        } => block_numbers
            .iter()
            .map(|block| FilterBlockOption::Range {
                from_block: Some((*block).into()),
                to_block: Some((*block).into()),
            })
            .collect(),
        BlockChunk {
            start_block: Some(start_block),
            end_block: Some(end_block),
            ..
        } => {
            let chunks = range_to_chunks(&start_block, &end_block, &log_request_size);
            chunks
                .iter()
                .map(|(start, end)| FilterBlockOption::Range {
                    from_block: Some((*start).into()),
                    to_block: Some((*end).into()),
                })
                .collect()
        }
        _ => panic!("invalid block range"),
    }
}

/// convert a range of numbers into a Vec of (start, end) chunk tuples
fn range_to_chunks(start: &u64, end: &u64, chunk_size: &u64) -> Vec<(u64, u64)> {
    let mut chunks = Vec::new();
    let mut chunk_start = *start;
    while chunk_start < *end {
        let chunk_end = (chunk_start + chunk_size).min(*end) - 1;
        chunks.push((chunk_start, chunk_end));
        chunk_start += chunk_size;
    }
    chunks
}

/// compute a hex hash of a slice of numbers
fn compute_numbers_hash(numbers: &[u64]) -> String {
    let joined_numbers = numbers
        .iter()
        .map(|num| num.to_string())
        .collect::<Vec<String>>()
        .join("");

    let hash: Digest = digest::digest(&digest::SHA256, joined_numbers.as_bytes());

    hex::encode(hash.as_ref())
}

/// convert a block chunk into a String representation
pub fn get_block_chunk_stub(chunk: &BlockChunk) -> String {
    match chunk {
        BlockChunk {
            block_numbers: Some(block_numbers),
            ..
        } => {
            let min = block_numbers.iter().min().unwrap();
            let max = block_numbers.iter().max().unwrap();
            let hash = compute_numbers_hash(block_numbers);
            format!("mixed_{}_to_{}_{}", min, max, &hash[0..8].to_string())
        }
        BlockChunk {
            start_block: Some(start_block),
            end_block: Some(end_block),
            ..
        } => format!("{}_to_{}", start_block, end_block),
        _ => panic!("invalid block range"),
    }
}

#[derive(Debug)]
pub enum BlockParseError {
    InvalidInput(String),
    // ParseError(std::num::ParseIntError),
}

/// parse block numbers to freeze
pub fn parse_block_inputs(inputs: &Vec<String>) -> Result<BlockChunk, BlockParseError> {
    match inputs.len() {
        1 => _process_block_input(inputs.get(0).unwrap(), true),
        _ => {
            let mut block_numbers: Vec<u64> = vec![];
            for input in inputs {
                let subchunk = _process_block_input(&input, false).unwrap();
                block_numbers.extend(subchunk.block_numbers.unwrap());
            }
            let block_chunk = BlockChunk {
                start_block: None,
                end_block: None,
                block_numbers: Some(block_numbers),
            };
            Ok(block_chunk)
        }
    }
}

fn _process_block_input(s: &str, as_range: bool) -> Result<BlockChunk, BlockParseError> {
    let parts: Vec<&str> = s.split(':').collect();
    match parts.len() {
        1 => {
            let block = parts
                .get(0)
                .ok_or("Missing number")
                .unwrap()
                .parse::<u64>()
                .unwrap();
            Ok(BlockChunk {
                start_block: None,
                end_block: None,
                block_numbers: Some(vec![block]),
            })
        }
        2 => {
            let start_block = parts
                .get(0)
                .ok_or("Missing first number")
                .unwrap()
                .parse::<u64>()
                .unwrap();
            let end_block = parts
                .get(1)
                .ok_or("Missing second number")
                .unwrap()
                .parse::<u64>()
                .unwrap();
            if as_range {
                Ok(BlockChunk {
                    start_block: Some(start_block),
                    end_block: Some(end_block),
                    block_numbers: None,
                })
            } else {
                Ok(BlockChunk {
                    start_block: None,
                    end_block: None,
                    block_numbers: Some((start_block..=end_block).collect()),
                })
            }
        }
        _ => {
            return Err(BlockParseError::InvalidInput(
                "blocks must be in format block_number or start_block:end_block".to_string(),
            ));
        }
    }
}
