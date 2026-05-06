use super::*;

/// the relay event stream produced by [`Hydrant::subscribe_repos`].
pub struct RelayEventStream(mpsc::Receiver<Result<bytes::Bytes, RelayStreamError>>);

#[derive(Debug, Clone, thiserror::Error)]
pub enum RelayStreamError {
    #[error("relay stream consumer too slow: {reason}")]
    ConsumerTooSlow { reason: String },
}

impl RelayStreamError {
    pub fn code(&self) -> &'static str {
        match self {
            Self::ConsumerTooSlow { .. } => "ConsumerTooSlow",
        }
    }
}

impl futures::Stream for RelayEventStream {
    type Item = Result<bytes::Bytes, RelayStreamError>;

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
    ///
    /// slow consumers receive [`RelayStreamError::ConsumerTooSlow`] before the stream terminates
    /// when possible.
    pub fn subscribe_repos(&self, cursor: Option<u64>) -> RelayEventStream {
        let (tx, rx) = mpsc::channel(500);
        let state = self.state.clone();
        let runtime = tokio::runtime::Handle::current();
        let opts = stream::StreamOptions::from_config(&self.config);

        std::thread::Builder::new()
            .name("hydrant-relay-stream".into())
            .spawn(move || {
                let _g = runtime.enter();
                relay_stream_thread(state, tx, cursor, opts);
            })
            .expect("failed to spawn relay stream thread");

        RelayEventStream(rx)
    }

    pub(crate) fn stream_send_timeout(&self) -> std::time::Duration {
        self.config.stream_send_timeout
    }
}
