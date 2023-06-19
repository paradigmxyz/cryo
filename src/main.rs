mod dataframes;
mod freeze;
mod gather;

use clap::Parser;
use ethers::prelude::*;

/// Command line arguments
#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    #[arg(required = true)]
    datatype: String,

    /// Name of the person to greet
    #[arg(short, long, default_value_t = 200)]
    max_concurrent_requests: usize,

    /// Number of times to greet
    #[arg(short, long, default_value = "17000000:17000100", num_args(0..))]
    blocks: Vec<String>,

    /// Chunk size (blocks per chunk)
    #[arg(short, long, default_value_t = 1000)]
    chunk_size: u64,

    /// RPC URL
    #[arg(short, long, default_value = "http://34.105.67.70:8545")]
    // #[arg(short, long, default_value = "ws://34.105.67.70")]
    rpc: String,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let opts = parse_opts();
    freeze::freeze(opts).await;
    Ok(())
}

/// parse options for running freeze
fn parse_opts() -> freeze::FreezeOpts {
    let args = Args::parse();

    // get block range
    let provider = Provider::<Http>::try_from(args.rpc).unwrap();
    let (start_block, end_block, block_numbers) = parse_block_inputs(&args.blocks).unwrap();
    freeze::FreezeOpts {
        datatype: args.datatype,
        provider: provider,
        max_concurrent_requests: Some(args.max_concurrent_requests),
        start_block: start_block,
        end_block: end_block,
        block_numbers: block_numbers,
        chunk_size: args.chunk_size,
    }
}

#[derive(Debug)]
enum BlockParseError {
    InvalidInput(String),
    // ParseError(std::num::ParseIntError),
}

/// parse block numbers to freeze
fn parse_block_inputs(inputs: &Vec<String>) -> Result<(Option<u64>, Option<u64>, Option<Vec<u64>>), BlockParseError> {
    // TODO: allow missing
    // TODO: allow 'latest'
    match inputs.len() {
        1 => _process_block_input(inputs.get(0).unwrap(), true),
        _ => {
            let mut block_numbers: Vec<u64> = vec![];
            for input in inputs {
                let (_s, _e, arg_block_numbers) = _process_block_input(&input, false).unwrap();
                block_numbers.extend(arg_block_numbers.unwrap());
            }
            Ok((None, None, Some(block_numbers)))
        }
    }
}

fn _process_block_input(s: &str, as_range: bool) -> Result<(Option<u64>, Option<u64>, Option<Vec<u64>>), BlockParseError> {
    let parts: Vec<&str> = s.split(':').collect();
    match parts.len() {
        1 => {
            let block = parts
                .get(0)
                .ok_or("Missing number")
                .unwrap()
                .parse::<u64>()
                .unwrap();
            Ok((None, None, Some(vec![block])))
        },
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
                Ok((Some(start_block), Some(end_block), None))
            }
            else {
                Ok((None, None, Some((start_block..=end_block).collect())))
            }
        },
        _ => {
            return Err(BlockParseError::InvalidInput(
                "blocks must be in format block_number or start_block:end_block"
                    .to_string(),
            ));
        }
    }
}

