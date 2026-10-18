//! cryo_cli is a cli for cryo_freeze

use clap::Parser;

pub use cryo_cli::{run, Args};
use eyre::Result;

#[tokio::main]
#[allow(unreachable_code)]
#[allow(clippy::needless_return)]
async fn main() -> Result<()> {
    tracing_subscriber::fmt().init();
    let args = Args::parse();
    match run(args).await {
        Ok(Some(freeze_summary)) if freeze_summary.errored.is_empty() => Ok(()),
        Ok(Some(_freeze_summary)) => std::process::exit(1),
        Ok(None) => Ok(()),
        Err(e) => {
            // handle debug build
            #[cfg(debug_assertions)]
            {
                return Err(eyre::Report::from(e))
            }

            // handle release build
            #[cfg(not(debug_assertions))]
            {
                tracing::error!("{}", e);
                std::process::exit(1);
            }
        }
    }
}
