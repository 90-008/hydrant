use super::*;

/// the relay event stream produced by [`Hydrant::subscribe_repos`].
pub struct RelayEventStream(mpsc::Receiver<bytes::Bytes>);

impl futures::Stream for RelayEventStream {
    type Item = bytes::Bytes;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        self.0.poll_recv(cx)
    }
}

impl Hydrant {
    /// subscribe to the relay's ordered `subscribeRepos` event stream.
    ///
    /// returns a [`RelayEventStream`] that yields pre-encoded CBOR binary frames
    /// ready to forward directly to ATProto clients via WebSocket.
    ///
    /// - if `cursor` is `None`, streaming starts from the current head (live tail only).
    /// - if `cursor` is `Some(seq)`, all persisted events from that seq onward are replayed first.
    pub fn subscribe_repos(&self, cursor: Option<u64>) -> RelayEventStream {
        let (tx, rx) = mpsc::channel(500);
        let state = self.state.clone();
        let runtime = tokio::runtime::Handle::current();

        std::thread::Builder::new()
            .name("hydrant-relay-stream".into())
            .spawn(move || {
                let _g = runtime.enter();
                relay_stream_thread(state, tx, cursor);
            })
            .expect("failed to spawn relay stream thread");

        RelayEventStream(rx)
    }
}
