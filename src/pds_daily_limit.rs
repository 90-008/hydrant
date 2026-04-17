use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

/// per-UTC-day counter for PDS additions via `requestCrawl`.
///
/// the in-memory state is initialised from the database on startup (see
/// [`crate::db::load_pds_daily_adds`]). the counter resets automatically when the UTC day
/// rolls over.
pub(crate) struct PdsDailyLimit {
    limit: Option<u64>,
    /// current UTC day index (unix seconds / 86400).
    day: AtomicU64,
    /// requestCrawl calls accepted on the current UTC day.
    count: AtomicU64,
}

impl PdsDailyLimit {
    /// construct from the previously-persisted `(day, count)` pair loaded from the database.
    /// if the stored day doesn't match today the count is treated as 0.
    pub(crate) fn new(limit: Option<u64>, stored: Option<(u64, u64)>) -> Self {
        let today = utc_day();
        let count = stored
            .filter(|(day, _)| *day == today)
            .map(|(_, count)| count)
            .unwrap_or(0);
        Self {
            limit,
            day: AtomicU64::new(today),
            count: AtomicU64::new(count),
        }
    }

    /// attempt to consume a daily slot.
    ///
    /// returns `(allowed, to_persist)`:
    /// - `allowed`: whether the request is permitted.
    /// - `to_persist`: when `Some((day, new_count))`, the caller must persist these values to
    ///   the database before returning success, so that a process crash cannot reset the counter
    ///   and allow the budget to be replayed. `None` when no limit is configured.
    ///
    /// when the UTC day rolls over the counter resets and a fresh quota starts.
    pub(crate) fn try_increment(&self) -> (bool, Option<(u64, u64)>) {
        let Some(limit) = self.limit else {
            return (true, None);
        };

        let today = utc_day();
        if self.day.load(Ordering::Relaxed) != today {
            self.count.store(0, Ordering::Relaxed);
            self.day.store(today, Ordering::Relaxed);
        }

        // fetch_add returns the value *before* the increment
        let prev = self.count.fetch_add(1, Ordering::Relaxed);
        if prev >= limit {
            // undo to avoid the counter drifting upwards on repeated rejections
            self.count.fetch_sub(1, Ordering::Relaxed);
            return (false, None);
        }

        let new_count = prev + 1;
        let day = self.day.load(Ordering::Relaxed);
        (true, Some((day, new_count)))
    }
}

fn utc_day() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
        / 86400
}
