use crate::types::Datatype;
use std::collections::HashMap;

/// Summary of freeze operation
pub struct FreezeSummary {
    /// number of chunks completed successfully
    pub n_completed: u64,
    /// number of chunks skipped
    pub n_skipped: u64,
    /// number of chunks that encountered an error
    pub n_errored: u64,
    /// paths_by_type
    pub paths_by_type: HashMap<Datatype, Vec<String>>,
}

pub(crate) trait FreezeSummaryAgg {
    fn aggregate(self) -> FreezeSummary;
}

impl FreezeSummaryAgg for Vec<FreezeChunkSummary> {
    fn aggregate(self) -> FreezeSummary {
        let mut n_completed: u64 = 0;
        let mut n_skipped: u64 = 0;
        let mut n_errored: u64 = 0;

        let mut paths_by_type = HashMap::new();
        for chunk_summary in self {
            if chunk_summary.skipped {
                n_skipped += 1;
            } else if chunk_summary.errored {
                n_errored += 1;
            } else {
                n_completed += 1;
            }
            for (datatype, path) in chunk_summary.paths {
                paths_by_type.entry(datatype).or_insert_with(Vec::new).push(path);
            }
        }

        FreezeSummary { n_completed, n_skipped, n_errored, paths_by_type }
    }
}

/// Summary of freezing a single chunk
pub struct FreezeChunkSummary {
    /// whether chunk was skipped
    pub skipped: bool,
    /// whether chunk encountered an error
    pub errored: bool,
    /// output paths
    pub paths: HashMap<Datatype, String>,
}

impl FreezeChunkSummary {
    pub(crate) fn success(paths: HashMap<Datatype, String>) -> FreezeChunkSummary {
        FreezeChunkSummary { skipped: false, errored: false, paths }
    }

    pub(crate) fn error(paths: HashMap<Datatype, String>) -> FreezeChunkSummary {
        FreezeChunkSummary { skipped: false, errored: true, paths }
    }

    pub(crate) fn skip(paths: HashMap<Datatype, String>) -> FreezeChunkSummary {
        FreezeChunkSummary { skipped: true, errored: false, paths }
    }
}
