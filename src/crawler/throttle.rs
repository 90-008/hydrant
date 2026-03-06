use scc::HashMap;
use std::future::Future;
use std::sync::Arc;
use std::sync::atomic::{AtomicI64, AtomicUsize, Ordering};
use std::time::Duration;
use tokio::sync::{Notify, Semaphore, SemaphorePermit};
use url::Url;

/// max concurrent in-flight requests per PDS before we start queuing
/// ref pds allows 10 requests per second... so 10 should be fine
const PER_PDS_CONCURRENCY: usize = 10;

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
}

impl State {
    fn new() -> Self {
        Self {
            throttled_until: AtomicI64::new(0),
            consecutive_failures: AtomicUsize::new(0),
            consecutive_timeouts: AtomicUsize::new(0),
            failure_notify: Notify::new(),
            semaphore: Semaphore::new(PER_PDS_CONCURRENCY),
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
    ///
    /// deliberately does NOT notify waiters — 429s are soft and tasks should exit
    /// naturally via the `Retry` result rather than being cancelled.
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
    pub fn record_failure(&self) -> Option<i64> {
        if self.is_throttled() {
            return None;
        }

        let failures = self
            .state
            .consecutive_failures
            .fetch_add(1, Ordering::AcqRel)
            + 1;

        // 30 min, 60 min, 120 min, ... capped at ~512 hours
        let base_minutes = 30i64;
        let exponent = (failures as u32).saturating_sub(1);
        let minutes = base_minutes * 2i64.pow(exponent.min(10));
        let until = chrono::Utc::now().timestamp() + minutes * 60;

        self.state.throttled_until.store(until, Ordering::Release);
        self.state.failure_notify.notify_waiters();

        Some(minutes)
    }

    /// returns current timeout duration — 3s, 6s, or 12s depending on prior timeouts.
    pub fn timeout(&self) -> Duration {
        let n = self.state.consecutive_timeouts.load(Ordering::Acquire);
        Duration::from_secs(3 * 2u64.pow(n.min(2) as u32))
    }

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
    /// used by `or_throttle` and the semaphore acquire select to cancel in-flight work.
    pub async fn wait_for_failure(&self) {
        loop {
            let notified = self.state.failure_notify.notified();
            if self.is_throttled() {
                return;
            }
            notified.await;
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
