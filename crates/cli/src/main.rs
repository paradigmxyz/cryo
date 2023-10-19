//! cryo_cli is a cli for cryo_freeze

use clap_cryo::Parser;

mod args;
mod parse;
mod remember;
mod run;

pub use args::Args;
use eyre::Result;

#[tokio::main]
#[allow(unreachable_code)]
#[allow(clippy::needless_return)]
async fn main() -> Result<()> {
    let args = Args::parse();
    match run::run(args).await {
        Ok(Some(freeze_summary)) if freeze_summary.errored.is_empty() => Ok(()),
        Ok(Some(_freeze_summary)) => std::process::exit(1),
        Ok(None) => Ok(()),
        Err(e) => {
            // handle release build
            #[cfg(debug_assertions)]
            {
                return Err(eyre::Report::from(e))
            }

            // handle debug build
            #[cfg(not(debug_assertions))]
            {
                println!("{}", e);
                std::process::exit(1);
            }
        }
    }
}
