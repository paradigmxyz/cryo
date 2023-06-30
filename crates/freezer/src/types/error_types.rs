use ethers::prelude::*;
use polars::prelude::*;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum FreezeError {
    #[error("Failed to create file path")]
    FilePathError(#[from] FileError),

    #[error("Task failed: {0}")]
    TaskFailed(#[source] tokio::task::JoinError),

    #[error("Collect error")]
    CollectError(#[from] CollectError),

    #[error("Progress bar error")]
    ProgressBarError(#[from] indicatif::style::TemplateError),
}

#[derive(Error, Debug)]
pub enum CollectError {
    #[error("Failed to get block: {0}")]
    ProviderError(#[source] ProviderError),

    #[error("Task failed: {0}")]
    TaskFailed(#[source] tokio::task::JoinError),

    #[error("Failed to convert to DataFrme: {0}")]
    PolarsError(#[from] PolarsError),

    #[error("Invalid number of topics")]
    InvalidNumberOfTopics,
}

#[derive(Error, Debug)]
pub enum ChunkError {

    #[error("Block chunk not valid")]
    InvalidChunk,

    #[error("Failed to create stub")]
    StubError,
}

#[derive(Error, Debug)]
pub enum FileError {
    #[error("Failed to build file path")]
    FilePathError(#[from] ChunkError),

    #[error("Error writing file")]
    FileWriteError,
}

