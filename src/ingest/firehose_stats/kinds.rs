use std::time::Duration;

#[derive(Clone, Copy, Debug)]
pub enum RelayMessageKind {
    Commit,
    Sync,
    Identity,
    Account,
}

#[derive(Clone, Copy, Debug)]
pub enum RepoStateLoadOutcome {
    Hit,
    Miss,
    Drop,
}

#[derive(Clone, Copy, Debug)]
pub enum HostAuthorityStatsOutcome {
    Authorized,
    WasStale,
    WrongHost,
    Error,
}

#[derive(Clone, Copy, Debug)]
pub enum ValidationStatsOutcome {
    Accepted,
    Stale,
    SigFailure,
    Rejected,
}

#[cfg_attr(not(feature = "firehose-diagnostics"), allow(dead_code))]
#[derive(Default)]
pub struct RelayShardTimings {
    pub process_message: Duration,
    pub stage_counts: Duration,
    pub stage_and_commit: Duration,
    pub apply_counts: Duration,
    pub broadcast: Duration,
    pub cursor: Duration,
    pub total: Duration,
}

/// instant used for diagnostics timing. compiles to a zero-sized no-op when
/// the `firehose-diagnostics` feature is disabled, so timing call sites stay
/// unconditional without paying for `Instant::now`.
#[cfg(feature = "firehose-diagnostics")]
pub(crate) use std::time::Instant as StatsInstant;

#[cfg(not(feature = "firehose-diagnostics"))]
#[derive(Clone, Copy)]
pub(crate) struct StatsInstant;

#[cfg(not(feature = "firehose-diagnostics"))]
impl StatsInstant {
    #[inline(always)]
    pub(crate) fn now() -> Self {
        Self
    }

    #[inline(always)]
    pub(crate) fn elapsed(&self) -> Duration {
        Duration::ZERO
    }
}
