use url::Url;

use super::FIREHOSE_CURSOR_PREFIX;

/// firehose cursor key for schema v1: `firehose_cursor|{host}`
pub fn firehose_cursor_key(host: &str) -> Vec<u8> {
    let mut key = FIREHOSE_CURSOR_PREFIX.to_vec();
    key.extend_from_slice(host.as_bytes());
    key
}

pub fn firehose_cursor_key_from_url(url: &Url) -> Vec<u8> {
    let host = url.host_str().unwrap_or("");
    let key = if let Some(port) = url.port() {
        format!("{host}:{port}")
    } else {
        host.to_string()
    };
    firehose_cursor_key(&key)
}
