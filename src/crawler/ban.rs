use scc::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicI64, AtomicUsize, Ordering};
use tokio::sync::Notify;
use url::Url;

#[derive(Clone)]
pub struct BanTracker {
    states: Arc<HashMap<Url, Arc<State>>>,
}

impl BanTracker {
    pub fn new() -> Self {
        Self {
            states: Arc::new(HashMap::new()),
        }
    }

    pub fn get_handle(&self, url: &Url) -> BanHandle {
        let state = self
            .states
            .entry_sync(url.clone())
            .or_insert_with(|| {
                Arc::new(State {
                    banned_until: AtomicI64::new(0),
                    consecutive_failures: AtomicUsize::new(0),
                    ban_notify: Notify::new(),
                })
            })
            .get()
            .clone();

        BanHandle { state }
    }
}

struct State {
    banned_until: AtomicI64,
    consecutive_failures: AtomicUsize,
    ban_notify: Notify,
}

pub struct BanHandle {
    state: Arc<State>,
}

impl BanHandle {
    pub fn is_banned(&self) -> bool {
        let until = self.state.banned_until.load(Ordering::Acquire);
        if until == 0 {
            return false;
        }
        let now = chrono::Utc::now().timestamp();
        now < until
    }

    pub fn record_success(&self) {
        self.state.consecutive_failures.store(0, Ordering::Release);
        self.state.banned_until.store(0, Ordering::Release);
    }

    // returns the amount of minutes banned if its a new ban
    pub fn record_failure(&self) -> Option<i64> {
        if self.is_banned() {
            return None;
        }

        let failures = self
            .state
            .consecutive_failures
            .fetch_add(1, Ordering::AcqRel)
            + 1;

        // start with 30 minutes, double each consecutive failure
        let base_minutes = 30;
        let exponent = (failures as u32).saturating_sub(1);
        let minutes = base_minutes * 2i64.pow(exponent.min(10));
        let now = chrono::Utc::now().timestamp();

        self.state
            .banned_until
            .store(now + minutes * 60, Ordering::Release);

        self.state.ban_notify.notify_waiters();

        Some(minutes)
    }

    pub async fn wait_for_ban(&self) {
        loop {
            let notified = self.state.ban_notify.notified();
            if self.is_banned() {
                return;
            }
            notified.await;
        }
    }
}
