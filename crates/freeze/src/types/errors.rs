/// error specifications
use ethers::prelude::*;
use polars::prelude::*;
use thiserror::Error;

/// return basic CollectError from str slice
pub fn err(message: &str) -> CollectError {
    CollectError::CollectError(message.to_string())
}

/// Error related to running freeze function
#[derive(Error, Debug)]
pub enum FreezeError {
    /// Error related to processing file path
    #[error(transparent)]
    FilePathError(#[from] FileError),

    /// Error related to joining a tokio task
    #[error("Task failed: {0}")]
    TaskFailed(#[source] tokio::task::JoinError),

    /// Error related to collecting data
    #[error(transparent)]
    CollectError(#[from] CollectError),

    /// Error related to progress bar
    #[error("Progress bar error")]
    ProgressBarError(#[from] indicatif::style::TemplateError),

    /// Parse error
    #[error(transparent)]
    ParseError(#[from] ParseError),

    /// Error from serializing report
    #[error("JSON error")]
    ReportSerializeError(#[from] serde_json::Error),

    /// Error from serializing report
    #[error("File creation error")]
    ReportFileCreationError(#[from] std::io::Error),

    /// General Error
    #[error("{0}")]
    GeneralError(String),
}

/// Error related to data collection
#[derive(Error, Debug)]
pub enum CollectError {
    /// General Collection error
    #[error("Collect failed: {0}")]
    CollectError(String),

    /// Parse error
    #[error(transparent)]
    ParseError(#[from] ParseError),

    /// Error related to provider operations
    #[error("Failed to get block: {0}")]
    ProviderError(#[source] ProviderError),

    /// Error related to tokio task
    #[error("Task failed: {0}")]
    TaskFailed(#[source] tokio::task::JoinError),

    /// Error related to polars operations
    #[error("Failed to convert to DataFrame: {0}")]
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

    /// Generic RPC Error
    #[error("RPC call error")]
    RPCError(String),
}

/// Error related to parsing
#[derive(Error, Debug)]
pub enum ParseError {
    /// Error related to parsing
    #[error("Parsing error: {0}")]
    ParseError(String),

    /// Error related to provider operations
    #[error("Failed to get block: {0}")]
    ProviderError(#[source] ProviderError),

    /// Parse int error
    #[error("Parsing error")]
    ParseIntError(#[from] std::num::ParseIntError),
}

/// Error performing a chunk operation
#[derive(Error, Debug)]
pub enum ChunkError {
    /// Error related to parsing
    #[error("Parsing error: {0}")]
    ChunkError(String),

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
