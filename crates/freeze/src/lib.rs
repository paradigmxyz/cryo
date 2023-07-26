//! cryo_freeze extracts EVM data to parquet, csv, or json

#![warn(missing_docs, unreachable_pub, unused_crate_dependencies)]
#![deny(unused_must_use, rust_2018_idioms)]
#![doc(test(
    no_crate_inject,
    attr(deny(warnings, rust_2018_idioms), allow(dead_code, unused_variables))
))]

mod collect;
mod datasets;
mod freeze;
mod types;

pub use collect::{collect, collect_multiple};
pub use freeze::freeze;
pub use types::*;
