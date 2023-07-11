/// error specifications
use ethers::prelude::*;
use polars::prelude::*;
use thiserror::Error;

/// Error related to running freeze function
#[derive(Error, Debug)]
pub enum FreezeError {
    /// Error related to processing file path
    #[error("Failed to create file path")]
    FilePathError(#[from] FileError),

    /// Error related to joining a tokio task
    #[error("Task failed: {0}")]
    TaskFailed(#[source] tokio::task::JoinError),

    /// Error related to collecting data
    #[error("Collect error")]
    CollectError(#[from] CollectError),

    /// Error related to progress bar
    #[error("Progress bar error")]
    ProgressBarError(#[from] indicatif::style::TemplateError),
}

/// Error related to data collection
#[derive(Error, Debug)]
pub enum CollectError {
    /// Error related to provider operations
    #[error("Failed to get block: {0}")]
    ProviderError(#[source] ProviderError),

    /// Error related to tokio task
    #[error("Task failed: {0}")]
    TaskFailed(#[source] tokio::task::JoinError),

    /// Error related to polars operations
    #[error("Failed to convert to DataFrme: {0}")]
    PolarsError(#[from] PolarsError),

    /// Error related to log topic filtering
    #[error("Invalid number of topics")]
    InvalidNumberOfTopics,

    /// Error related to bad schema
    #[error("Bad schema specified")]
    BadSchemaError,

    /// Error related to too many requests
    #[error("try using a rate limit with --requests-per-second or limiting max concurrency with --max-concurrent-requests")]
    TooManyRequestsError,
}

/// Error performing a chunk operation
#[derive(Error, Debug)]
pub enum ChunkError {
    /// Error in chunk specification
    #[error("Block chunk not valid")]
    InvalidChunk,

    /// Error in creating a chunk stub
    #[error("Failed to create stub")]
    StubError,
}

/// Error related to file operations
#[derive(Error, Debug)]
pub enum FileError {
    /// Error in creating filepath
    #[error("Failed to build file path")]
    FilePathError(#[from] ChunkError),

    /// File path not given
    #[error("File path not given")]
    NoFilePathError(String),

    /// Error in writing file
    #[error("Error writing file")]
    FileWriteError,
}
