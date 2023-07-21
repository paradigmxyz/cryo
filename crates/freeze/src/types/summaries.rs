/// Summary of freeze operation
pub struct FreezeSummary {
    /// number of chunks completed successfully
    pub n_completed: u64,
    /// number of chunks skipped
    pub n_skipped: u64,
    /// number of chunks that encountered an error
    pub n_errored: u64,
}

pub(crate) trait FreezeSummaryAgg {
    fn aggregate(self) -> FreezeSummary;
}

impl FreezeSummaryAgg for Vec<FreezeChunkSummary> {
    fn aggregate(self) -> FreezeSummary {
        let mut n_completed: u64 = 0;
        let mut n_skipped: u64 = 0;
        let mut n_errored: u64 = 0;

        for chunk_summary in self {
            if chunk_summary.skipped {
                n_skipped += 1;
            } else if chunk_summary.errored {
                n_errored += 1;
            } else {
                n_completed += 1;
            }
        }

        FreezeSummary {
            n_completed,
            n_skipped,
            n_errored,
        }
    }
}

/// Summary of freezing a single chunk
pub struct FreezeChunkSummary {
    /// whether chunk was skipped
    pub skipped: bool,
    /// whether chunk encountered an error
    pub errored: bool,
}

impl FreezeChunkSummary {
    pub(crate) fn success() -> FreezeChunkSummary {
        FreezeChunkSummary {
            skipped: false,
            errored: false,
        }
    }

    pub(crate) fn error() -> FreezeChunkSummary {
        FreezeChunkSummary {
            skipped: false,
            errored: true,
        }
    }

    pub(crate) fn skip() -> FreezeChunkSummary {
        FreezeChunkSummary {
            skipped: true,
            errored: false,
        }
    }
}
