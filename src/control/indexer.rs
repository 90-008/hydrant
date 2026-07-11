use super::*;

#[cfg(feature = "indexer_stream")]
/// a stream of [`Event`]s. returned by [`Hydrant::subscribe`].
///
/// implements [`futures::Stream`] and can be used with `StreamExt::next`,
/// `while let Some(item) = stream.next().await`, `forward`, etc.
/// the stream terminates when the underlying channel closes (i.e. hydrant shuts down).
pub struct EventStream(mpsc::Receiver<Result<Event, StreamError>>);

#[cfg(feature = "indexer_stream")]
#[derive(Debug, Clone, thiserror::Error)]
pub enum StreamError {
    #[error("stream consumer too slow: {reason}")]
    ConsumerTooSlow { reason: String },
}

#[cfg(feature = "indexer_stream")]
impl StreamError {
    pub fn code(&self) -> &'static str {
        match self {
            Self::ConsumerTooSlow { .. } => "ConsumerTooSlow",
        }
    }
}

#[cfg(feature = "indexer_stream")]
impl Stream for EventStream {
    type Item = Result<Event, StreamError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.0.poll_recv(cx)
    }
}

/// runtime control over the backfill worker component.
///
/// the backfill worker fetches full repo CAR files from each repo's PDS for any
/// repository in the pending queue, parses the MST, and inserts all matching records
/// into the database. concurrency is bounded by `HYDRANT_BACKFILL_CONCURRENCY_LIMIT`.
#[derive(Clone)]
pub struct BackfillHandle(Arc<AppState>);

impl BackfillHandle {
    pub(crate) fn new(state: Arc<AppState>) -> Self {
        Self(state)
    }

    /// enable the backfill worker, no-op if already enabled.
    pub fn enable(&self) {
        self.0.backfill_enabled.send_replace(true);
    }
    /// disable the backfill worker, in-flight repos complete before pausing.
    pub fn disable(&self) {
        self.0.backfill_enabled.send_replace(false);
    }
    /// returns the current enabled state of the backfill worker.
    pub fn is_enabled(&self) -> bool {
        *self.0.backfill_enabled.borrow()
    }
}

#[cfg(feature = "indexer_stream")]
impl Hydrant {
    /// subscribe to the ordered event stream.
    ///
    /// returns an [`EventStream`] that implements [`futures::Stream`].
    ///
    /// - if `cursor` is `None`, streaming starts from the current head (live tail only).
    /// - if `cursor` is `Some(id)`, all persisted `record` events from that ID onward are
    ///   replayed first, then the stream will switch to live tailing.
    ///
    /// `identity` and `account` events are ephemeral and are never replayed from a cursor,
    /// only live ones are delivered. use [`ReposControl::info`] to fetch current state for
    /// a specific repository.
    ///
    /// multiple concurrent subscribers each receive a full independent copy of the stream.
    /// the stream ends when the `EventStream` is dropped. slow consumers receive
    /// [`StreamError::ConsumerTooSlow`] before the stream terminates when possible.
    pub fn subscribe(&self, cursor: Option<u64>) -> EventStream {
        let (tx, rx) = mpsc::channel(stream::STREAM_CHANNEL_CAPACITY);
        let state = self.state.clone();
        let runtime = tokio::runtime::Handle::current();
        let opts = stream::StreamOptions::from_config(&self.config);

        std::thread::Builder::new()
            .name("hydrant-stream".into())
            .spawn(move || {
                let _g = runtime.enter();
                event_stream_thread(state, tx, cursor, opts);
            })
            .expect("failed to spawn stream thread");

        EventStream(rx)
    }

    #[cfg(feature = "indexer_stream")]
    #[doc(hidden)]
    pub fn seed_events_for_bench(&self, count: usize) -> usize {
        use crate::db::keys;
        use crate::db::types::{DbAction, DbRkey, DbTid, TrimmedDid};
        use crate::types::{StoredData, StoredEvent};
        use jacquard_common::types::did::Did;
        use jacquard_common::{CowStr, IntoStatic};
        use std::sync::atomic::Ordering;

        let db = &self.state.db;
        let mut batch = db.inner.batch();
        let mut total_bytes = 0usize;

        let did_str = "did:plc:aaaaaaaaaaaaaaaaaaaaaaaaaaaa";
        let did = Did::new(did_str).expect("valid plc did");
        let trimmed = TrimmedDid::from(&did).into_static();
        let rev = DbTid::new_from_bytes([0u8; 8]);
        let collection = CowStr::Borrowed("app.bsky.feed.post").into_static();

        for i in 0..count {
            let event_id = db.stream.next_event_id.fetch_add(1, Ordering::SeqCst);
            let rkey = DbRkey::Str(smol_str::format_smolstr!("r{i}"));
            let evt = StoredEvent {
                live: false,
                did: trimmed.clone(),
                rev,
                collection: collection.clone(),
                rkey,
                action: DbAction::Create,
                data: StoredData::Nothing,
            };
            let bytes = rmp_serde::to_vec(&evt).expect("msgpack serialization cannot fail");
            total_bytes += bytes.len();
            batch.insert(&db.stream.events, keys::event_key(event_id), bytes);
        }

        batch.commit().expect("failed to commit events batch");
        total_bytes
    }
}
