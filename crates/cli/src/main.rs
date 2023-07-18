//! cryo_cli is a cli for cryo_freeze

use clap::Parser;

mod args;
mod parse;
mod run;
mod summaries;

pub use args::Args;
use eyre::Result;

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    run::run(args).await
}
