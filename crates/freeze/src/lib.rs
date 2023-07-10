//! cryo_freeze extracts EVM data to parquet, csv, or json

#![warn(missing_docs, unreachable_pub, unused_crate_dependencies)]
#![deny(unused_must_use, rust_2018_idioms)]
#![doc(test(
    no_crate_inject,
    attr(deny(warnings, rust_2018_idioms), allow(dead_code, unused_variables))
))]

mod chunks;
mod datasets;

#[macro_use]
mod dataframes;

mod freeze;
mod outputs;
mod types;

pub use chunks::ChunkAgg;
pub use freeze::freeze;
pub use types::*;
