//! cryo_cli is a cli for cryo_freeze

#![warn(missing_docs, unreachable_pub, unused_crate_dependencies)]
#![deny(unused_must_use, rust_2018_idioms)]
#![doc(test(
    no_crate_inject,
    attr(deny(warnings, rust_2018_idioms), allow(dead_code, unused_variables))
))]

mod args;
mod parse;
mod summaries;

use eyre::Result;
use std::time::SystemTime;
pub use args::Args;
pub use parse::parse_opts;

#[tokio::main]
async fn main() -> Result<()> {
    let t_start = SystemTime::now();
    let opts = match parse::parse_opts().await {
        Ok(opts) => opts,
        Err(e) => return Err(e),
    };
    let t_parse_done = SystemTime::now();
    summaries::print_cryo_summary(&opts);
    if opts.dry_run {
        println!("\n\n[dry run, exiting]");
    } else {
        summaries::print_header("\n\ncollecting data");
        match cryo_freeze::freeze(opts.clone()).await {
            Ok(freeze_summary) => {
                let t_data_done = SystemTime::now();
                println!("...done\n\n");
                summaries::print_cryo_conclusion(
                    t_start,
                    t_parse_done,
                    t_data_done,
                    &opts,
                    &freeze_summary,
                );
            }
            Err(e) => {
                println!("{}", e)
            }
        }
    };
    Ok(())
}
