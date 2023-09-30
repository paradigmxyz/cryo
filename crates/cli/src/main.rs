//! cryo_cli is a cli for cryo_freeze

use clap_cryo::Parser;

mod args;
mod parse;
mod run;

pub use args::Args;
use eyre::Result;

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    match run::run(args).await {
        Ok(Some(freeze_summary)) if freeze_summary.errored.is_empty() => Ok(()),
        Ok(Some(_freeze_summary)) => Err(eyre::Error::msg("Some chunks failed")),
        Ok(None) => Ok(()),
        Err(e) => Err(eyre::Report::from(e)),
    }
}
