use super::*;

#[cfg(feature = "indexer_stream")]
/// a stream of [`Event`]s. returned by [`Hydrant::subscribe`].
///
/// implements [`futures::Stream`] and can be used with `StreamExt::next`,
/// `while let Some(evt) = stream.next().await`, `forward`, etc.
/// the stream terminates when the underlying channel closes (i.e. hydrant shuts down).
pub struct EventStream(mpsc::Receiver<Event>);

#[cfg(feature = "indexer_stream")]
impl Stream for EventStream {
    type Item = Event;

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
    /// the stream ends when the `EventStream` is dropped.
    pub fn subscribe(&self, cursor: Option<u64>) -> EventStream {
        let (tx, rx) = mpsc::channel(500);
        let state = self.state.clone();
        let runtime = tokio::runtime::Handle::current();

        std::thread::Builder::new()
            .name("hydrant-stream".into())
            .spawn(move || {
                let _g = runtime.enter();
                event_stream_thread(state, tx, cursor);
            })
            .expect("failed to spawn stream thread");

        EventStream(rx)
    }
}
