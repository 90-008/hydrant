use crate::config::RateTier;
use parking_lot::Mutex;
use scc::HashMap;
use std::future::Future;
use std::sync::Arc;
use std::sync::atomic::{AtomicI64, AtomicUsize, Ordering};
use std::time::{Duration, Instant};
use tokio::sync::{Notify, Semaphore, SemaphorePermit};
use url::Url;

/// max concurrent in-flight requests per PDS before we start queuing
/// ref pds allows 10 requests per second... so 10 should be fine
const PER_PDS_CONCURRENCY: usize = 10;

// per second, hour and day
const DURATIONS: [Duration; 3] = [
    Duration::from_secs(1),
    Duration::from_secs(3600),
    Duration::from_secs(86400),
];

#[derive(Clone)]
pub struct Throttler {
    states: Arc<HashMap<Url, Arc<State>>>,
}

impl Throttler {
    pub fn new() -> Self {
        Self {
            states: Arc::new(HashMap::new()),
        }
    }

    pub async fn get_handle(&self, url: &Url) -> ThrottleHandle {
        let state = self
            .states
            .entry_async(url.clone())
            .await
            .or_insert_with(|| Arc::new(State::new()))
            .get()
            .clone();

        ThrottleHandle { state }
    }

    /// drop entries with no active throttle and no consecutive failures.
    pub async fn evict_clean(&self) {
        self.states
            .retain_async(|_, v| {
                v.throttled_until.load(Ordering::Acquire) != 0
                    || v.consecutive_failures.load(Ordering::Acquire) != 0
            })
            .await;
    }
}

struct State {
    throttled_until: AtomicI64,
    consecutive_failures: AtomicUsize,
    consecutive_timeouts: AtomicUsize,
    /// only fires on hard failures (timeout, TLS, bad gateway, etc).
    /// ratelimits do NOT fire this — they just store `throttled_until` and
    /// let tasks exit naturally, deferring to the background retry loop.
    failure_notify: Notify,
    semaphore: Semaphore,
    rate_limiter: RateLimiter,
}

impl State {
    fn new() -> Self {
        Self {
            throttled_until: AtomicI64::new(0),
            consecutive_failures: AtomicUsize::new(0),
            consecutive_timeouts: AtomicUsize::new(0),
            failure_notify: Notify::new(),
            semaphore: Semaphore::new(PER_PDS_CONCURRENCY),
            rate_limiter: RateLimiter::new(),
        }
    }
}

pub struct ThrottleHandle {
    state: Arc<State>,
}

impl ThrottleHandle {
    pub fn is_throttled(&self) -> bool {
        let until = self.state.throttled_until.load(Ordering::Acquire);
        until != 0 && chrono::Utc::now().timestamp() < until
    }

    /// the unix timestamp at which this throttle expires (0 if not throttled).
    pub fn throttled_until(&self) -> i64 {
        self.state.throttled_until.load(Ordering::Acquire)
    }

    pub fn record_success(&self) {
        self.state.consecutive_failures.store(0, Ordering::Release);
        self.state.consecutive_timeouts.store(0, Ordering::Release);
        self.state.throttled_until.store(0, Ordering::Release);
    }

    /// called on a 429 response. `retry_after_secs` comes from the `Retry-After`
    /// header if present; falls back to 60s. uses `fetch_max` so concurrent callers
    /// don't race each other back to a shorter window.
    pub fn record_ratelimit(&self, retry_after_secs: Option<u64>) {
        let secs = retry_after_secs.unwrap_or(60) as i64;
        let until = chrono::Utc::now().timestamp() + secs;
        self.state
            .throttled_until
            .fetch_max(until, Ordering::AcqRel);
    }

    /// called on hard failures (timeout, TLS error, bad gateway, etc).
    /// returns throttle duration in minutes if this is a *new* throttle,
    /// and notifies all in-flight tasks to cancel immediately.
    pub fn record_failure(&self) -> Option<u64> {
        if self.is_throttled() {
            return None;
        }

        let failures = self
            .state
            .consecutive_failures
            .fetch_add(1, Ordering::AcqRel)
            + 1;

        // 30 min, 60 min, 120 min, ... capped at ~512 hours
        let base_minutes = 30u64;
        let exponent = (failures as u32).saturating_sub(1);
        let minutes = base_minutes * 2u64.pow(exponent.min(10));
        let until = chrono::Utc::now().timestamp() + (minutes * 60) as i64;

        self.state.throttled_until.store(until, Ordering::Release);
        self.state.failure_notify.notify_waiters();

        Some(minutes)
    }

    /// returns current timeout duration — 3s, 6s, or 12s depending on prior timeouts.
    pub fn timeout(&self) -> Duration {
        let n = self.state.consecutive_timeouts.load(Ordering::Acquire);
        Duration::from_secs(3 * 2u64.pow(n.min(2) as u32))
    }

    /// returns whether the timeout attempts are exhausted
    pub fn record_timeout(&self) -> bool {
        let timeouts = self
            .state
            .consecutive_timeouts
            .fetch_add(1, Ordering::AcqRel)
            + 1;
        timeouts > 2
    }

    /// acquire a concurrency slot for this PDS. hold the returned permit
    /// for the duration of the request.
    pub async fn acquire(&self) -> SemaphorePermit<'_> {
        self.state
            .semaphore
            .acquire()
            .await
            .expect("throttle semaphore unexpectedly closed")
    }

    /// resolves when this PDS gets a hard failure notification.
    pub async fn wait_for_failure(&self) {
        loop {
            let notified = self.state.failure_notify.notified();
            if self.is_throttled() {
                return;
            }
            notified.await;
        }
    }

    /// waits until the rate tier's limits allow more events for this PDS.
    /// sleeps precisely until the most restrictive window opens rather than polling.
    pub async fn wait_for_allow(&self, num_accounts: u64, tier: &RateTier) {
        let limits = limits_for(num_accounts, tier);
        while let Some(wait) = self.state.rate_limiter.try_acquire(limits) {
            tokio::time::sleep(wait).await;
        }
    }
}

fn limits_for(num_accounts: u64, tier: &RateTier) -> [u64; 3] {
    let per_sec = tier
        .per_second_base
        .max((num_accounts as f64 * tier.per_second_account_mul) as u64);
    [per_sec, tier.per_hour, tier.per_day]
}

struct WindowState {
    count: u64,
    prev_count: u64,
    window_start: Instant,
}

impl WindowState {
    fn new() -> Self {
        Self {
            count: 0,
            prev_count: 0,
            window_start: Instant::now(),
        }
    }

    fn rotate(&mut self, dur: Duration) {
        let elapsed = self.window_start.elapsed();
        if elapsed >= dur {
            let n = (elapsed.as_nanos() / dur.as_nanos()).max(1) as u32;
            self.prev_count = if n == 1 { self.count } else { 0 };
            self.count = 0;
            self.window_start += dur * n;
        }
    }

    /// returns how long to sleep before this window would allow one more event.
    /// Duration::ZERO means allow now.
    fn wait_needed(&self, dur: Duration, limit: u64) -> Duration {
        let elapsed = self.window_start.elapsed();
        let remaining = dur.saturating_sub(elapsed);
        let weight = remaining.as_secs_f64() / dur.as_secs_f64();
        let effective = self.count as f64 + self.prev_count as f64 * weight;

        if effective < limit as f64 {
            return Duration::ZERO;
        }

        if self.prev_count == 0 || self.count as f64 >= limit as f64 {
            // must wait for a full window rotation
            remaining + Duration::from_millis(1)
        } else {
            let secs = remaining.as_secs_f64()
                - dur.as_secs_f64() * (limit as f64 - self.count as f64) / self.prev_count as f64;
            Duration::from_secs_f64(secs.max(0.0)) + Duration::from_micros(500)
        }
    }
}

struct RateLimiter {
    // parking_lot::Mutex — uncontended path never touches the kernel
    windows: Mutex<[WindowState; 3]>,
}

impl RateLimiter {
    fn new() -> Self {
        Self {
            windows: Mutex::new([WindowState::new(), WindowState::new(), WindowState::new()]),
        }
    }

    /// returns None if the slot was acquired, or Some(sleep_for) if limited.
    fn try_acquire(&self, limits: [u64; 3]) -> Option<Duration> {
        let mut windows = self.windows.lock();

        windows
            .iter_mut()
            .zip(DURATIONS)
            .for_each(|(w, d)| w.rotate(d));

        let max_wait = windows
            .iter()
            .zip(DURATIONS)
            .zip(limits)
            .map(|((w, dur), limit)| w.wait_needed(dur, limit))
            .max()
            .unwrap_or(Duration::ZERO);

        if max_wait.is_zero() {
            windows.iter_mut().for_each(|w| w.count += 1);
            None
        } else {
            Some(max_wait)
        }
    }
}

/// adds a method for racing the future against a hard-failure notification.
#[allow(async_fn_in_trait)]
pub trait OrFailure<T, E>: Future<Output = Result<T, E>> {
    async fn or_failure(
        self,
        handle: &ThrottleHandle,
        on_throttle: impl FnOnce() -> E,
    ) -> Result<T, E>
    where
        Self: Sized,
    {
        tokio::select! {
            res = self => res,
            _ = handle.wait_for_failure() => Err(on_throttle()),
        }
    }
}

impl<T, E, F: Future<Output = Result<T, E>>> OrFailure<T, E> for F {}
