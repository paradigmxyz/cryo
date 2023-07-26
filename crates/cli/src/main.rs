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
    match run::run(args).await {
        Ok(Some(_freeze_summary)) => Ok(()),
        Ok(None) => Ok(()),
        Err(e) => Err(eyre::Report::from(e)),
    }
}
