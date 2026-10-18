//! cryo_cli is a cli for cryo_freeze

use clap_cryo::Parser;
use eyre::{Report, Result};

mod args;
mod parse;
mod remember;
mod run;

pub use args::Args;

/// Main entry point for the cryo_cli application.
/// Initializes the application, parses arguments, and executes the run logic.
/// Error handling is consistent across different build configurations.
#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    match run::run(args).await {
        Ok(Some(freeze_summary)) if freeze_summary.errored.is_empty() => Ok(()),
        Ok(Some(_)) => handle_error(),
        Err(e) => handle_error_with_report(e.into()), // Converting using 'into' method
        Ok(None) => Ok(()),
    }
}

/// Handles errors by exiting the application with a non-zero status code.
/// This function is used for cases where detailed error information is not available or necessary.
fn handle_error() -> Result<()> {
    std::process::exit(1);
}

/// Handles errors with detailed reporting.
/// This function is used in debug builds to provide more information about the error.
#[cfg(debug_assertions)]
fn handle_error_with_report(e: eyre::Error) -> Result<()> {
    Err(Report::from(e))
}

/// Handles errors without detailed reporting.
/// This function is used in release builds where detailed error information is not required.
/// Ensures that error messages are printed before exiting the application.
#[cfg(not(debug_assertions))]
fn handle_error_with_report(e: eyre::Error) -> Result<()> {
    println!("{}", e);
    std::process::exit(1);
}
