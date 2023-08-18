use crate::types::Datatype;
use std::{collections::HashMap, path::PathBuf};

/// Summary of freeze operation
#[derive(serde::Serialize, Debug)]
pub struct FreezeSummary {
    /// number of chunks completed successfully
    pub n_completed: u64,
    /// number of chunks skipped
    pub n_skipped: u64,
    /// number of chunks that encountered an error
    pub n_errored: u64,
    /// paths
    pub paths: HashMap<Datatype, Vec<PathBuf>>,
}

pub(crate) trait FreezeSummaryAgg {
    fn aggregate(self) -> FreezeSummary;
}

impl FreezeSummaryAgg for Vec<FreezeChunkSummary> {
    fn aggregate(self) -> FreezeSummary {
        let mut n_completed: u64 = 0;
        let mut n_skipped: u64 = 0;
        let mut n_errored: u64 = 0;

        let mut paths = HashMap::new();
        for chunk_summary in self {
            if chunk_summary.skipped {
                n_skipped += 1;
            } else if chunk_summary.errored {
                n_errored += 1;
            } else {
                n_completed += 1;
            }
            for (datatype, path) in chunk_summary.paths {
                paths.entry(datatype).or_insert_with(Vec::new).push(path);
            }
        }

        FreezeSummary { n_completed, n_skipped, n_errored, paths }
    }
}

/// Summary of freezing a single chunk
pub struct FreezeChunkSummary {
    /// whether chunk was skipped
    pub skipped: bool,
    /// whether chunk encountered an error
    pub errored: bool,
    /// output paths
    pub paths: HashMap<Datatype, PathBuf>,
}

impl FreezeChunkSummary {
    pub(crate) fn success(paths: HashMap<Datatype, PathBuf>) -> FreezeChunkSummary {
        FreezeChunkSummary { skipped: false, errored: false, paths }
    }

    pub(crate) fn error(paths: HashMap<Datatype, PathBuf>) -> FreezeChunkSummary {
        FreezeChunkSummary { skipped: false, errored: true, paths }
    }

    pub(crate) fn skip(paths: HashMap<Datatype, PathBuf>) -> FreezeChunkSummary {
        FreezeChunkSummary { skipped: true, errored: false, paths }
    }
}
