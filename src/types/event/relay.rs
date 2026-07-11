#[derive(Clone)]
pub(crate) enum RelayBroadcast {
    Persisted(#[allow(dead_code)] u64),
    #[allow(dead_code)]
    Ephemeral(u64, bytes::Bytes),
}
