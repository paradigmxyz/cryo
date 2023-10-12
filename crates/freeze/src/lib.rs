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
mod multi_datasets;
mod types;

pub use collect::collect;
pub use datasets::*;
pub use freeze::freeze;
pub use multi_datasets::*;
pub use types::*;
