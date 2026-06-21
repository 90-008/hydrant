use std::convert::Infallible;
use std::time::Duration;

use axum::http::Uri;
use bytes::Bytes;
use futures::{SinkExt, StreamExt};
use jacquard_common::error::DecodeError;
use miette::Diagnostic;
use smol_str::format_smolstr;
use thiserror::Error;
use tokio_websockets::{ClientBuilder, Message as WsMsg, WebSocketStream};
use tracing::trace;
use url::Url;

pub mod types;
pub mod codec;

#[allow(unused_imports)]
pub use types::{
    Datetime, RepoOpAction, RepoOp, Commit, Identity, AccountStatus, Account, Sync, InfoName,
    Info, SubscribeReposMessage,
};
pub use codec::decode_frame;

#[cfg(feature = "relay")]
pub use codec::{encode_frame, encode_error_frame};

#[derive(Debug, Error, Diagnostic)]
pub enum FirehoseError {
    #[error("websocket error: {0}")]
    WebSocket(#[from] tokio_websockets::Error),
    #[error("unknown scheme: {0}")]
    UnknownScheme(String),
    #[error("invalid websocket uri: {0}")]
    InvalidUri(String),
    #[error("decode error: {0}")]
    Decode(#[from] DecodeError),
    #[error("empty frame")]
    EmptyFrame,
    #[error("relay error {error}: {message:?}")]
    RelayError {
        error: String,
        message: Option<String>,
    },
    #[error("unknown op: {0}")]
    UnknownOp(i64),
    #[error("missing type in header")]
    MissingType,
    #[error("unknown event type: {0}")]
    UnknownType(String),
    #[error("cbor decode error: {0}")]
    Cbor(String),
    #[error("stream closed: {code}: {reason}")]
    StreamClosed { code: u16, reason: String },
    #[error("tcp layer dropped")]
    TcpDropped,
    #[error("future cursor")]
    FutureCursor,
}

impl From<serde_ipld_dagcbor::DecodeError<Infallible>> for FirehoseError {
    fn from(e: serde_ipld_dagcbor::DecodeError<Infallible>) -> Self {
        Self::Cbor(e.to_string())
    }
}

pub struct FirehoseStream {
    ws: WebSocketStream<tokio_websockets::MaybeTlsStream<tokio::net::TcpStream>>,
    ping_timer: tokio::time::Interval,
}

impl FirehoseStream {
    pub async fn connect(mut relay: Url, cursor: Option<i64>) -> Result<Self, FirehoseError> {
        let scheme = match relay.scheme() {
            "https" | "wss" => "wss",
            "http" | "ws" => "ws",
            x => return Err(FirehoseError::UnknownScheme(x.to_string())),
        };
        relay.set_scheme(scheme).expect("to be valid url");
        relay.set_path("/xrpc/com.atproto.sync.subscribeRepos");
        let cursor = cursor.map(|c| format_smolstr!("cursor={c}"));
        relay.set_query(cursor.as_deref());

        let uri: Uri = relay
            .as_str()
            .parse()
            .map_err(|e| FirehoseError::InvalidUri(format!("{e}")))?;

        let (ws, _) = ClientBuilder::from_uri(uri).connect().await?;

        let mut ping_timer = tokio::time::interval(Duration::from_secs(45));
        ping_timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        ping_timer.tick().await; // consume the first tick

        Ok(Self { ws, ping_timer })
    }

    /// gets the next message bytes from the firehose
    /// none means the stream is closed
    pub async fn next(&mut self) -> Result<Bytes, FirehoseError> {
        loop {
            let res = tokio::select! {
                _ = self.ping_timer.tick() => {
                    self.ping().await?;
                    continue;
                }
                res = self.ws.next() => {
                    res.map(|m| m.map_err(Into::into))
                        .unwrap_or(Err(FirehoseError::TcpDropped))?
                }
            };
            match res {
                msg if msg.is_binary() => {
                    let bytes: Bytes = msg.into_payload().into();
                    if bytes.is_empty() {
                        return Err(FirehoseError::EmptyFrame);
                    }
                    return Ok(bytes);
                }
                // if ws closed treat it as an error, since why would a host close the stream??
                // TODO: treat hosts that return these as offline ??????
                msg if msg.is_close() => {
                    let (code, reason) = msg.as_close().map_or_else(
                        || (0, "no reason".to_string()),
                        |(code, reason)| (code.into(), reason.to_owned()),
                    );
                    return Err(FirehoseError::StreamClosed { code, reason });
                }
                msg if msg.is_ping() => self.ws.send(WsMsg::pong(msg.into_payload())).await?,
                msg if msg.is_pong() => continue,
                x => {
                    trace!(msg = ?x, "host sent unexpected message");
                    continue;
                }
            }
        }
    }

    async fn ping(&mut self) -> Result<(), FirehoseError> {
        self.ws
            .send(WsMsg::ping(Bytes::new()))
            .await
            .map_err(Into::into)
    }
}

#[cfg(test)]
mod test {
    #[cfg(feature = "relay")]
    use super::FirehoseError;
    use super::{SubscribeReposMessage, decode_frame};
    #[cfg(feature = "relay")]
    use jacquard_common::types::{
        cid::CidLink,
        string::{Did, Tid},
    };
    #[cfg(feature = "relay")]
    use super::types::{RepoOp, Datetime};

    #[cfg(feature = "relay")]
    #[derive(serde::Serialize)]
    #[serde(rename_all = "camelCase")]
    struct CommitWithoutTooBig<'a> {
        #[serde(borrow)]
        blobs: Vec<CidLink<'a>>,
        #[serde(with = "jacquard_common::serde_bytes_helper")]
        blocks: bytes::Bytes,
        #[serde(borrow)]
        commit: CidLink<'a>,
        #[serde(borrow)]
        ops: Vec<RepoOp<'a>>,
        #[serde(skip_serializing_if = "Option::is_none")]
        #[serde(default)]
        #[serde(borrow)]
        prev_data: Option<CidLink<'a>>,
        rebase: bool,
        #[serde(borrow)]
        repo: Did<'a>,
        rev: Tid,
        seq: i64,
        since: Option<Tid>,
        time: Datetime,
    }

    #[test]
    fn test_decode_account() {
        const FRAME: &[u8] = b"omF0aCNhY2NvdW50Ym9wAaRjZGlkeCBkaWQ6cGxjOjNuNDNncWY2YTZua3J3MzU1cnNjNnJ0ZGNzZXEbAAAAAlP5Zt5kdGltZXgbMjAyNi0wMi0yNFQxNDowNToyMC41MjE1NDY5ZmFjdGl2ZfU=";
        let bytes = data_encoding::BASE64.decode(FRAME).unwrap();
        let msg = decode_frame(&bytes).unwrap();
        assert!(matches!(msg, SubscribeReposMessage::Account(_)));
    }

    /// regression: some relays send `since: ""` (empty string) instead of null/absent for the initial commit.
    /// this should decode cleanly with `since = None`.
    /// TODO: is this behaviour we should reject?
    #[test]
    fn test_decode_commit_empty_since() {
        const FRAME: &[u8] = b"omF0ZyNjb21taXRib3ABq2NvcHOAY3Jldm0zbWdkeWNmdWIyeTIyY3NlcRsAAAACZ8sHJmRyZXBveCBkaWQ6cGxjOmpxM3p2cmI1ZXdnMnFndXA3M3Fzb3V6ZWR0aW1leBsyMDI2LTAzLTA1VDE1OjQ3OjU0LjcxNjMyOFplYmxvYnOAZXNpbmNlYGZibG9ja3NZAUk6omVyb290c4HYKlglAAFxEiAlaO/rjabPL4/e2QlxkoxzCCwv69hE4P3Vdxpv7f6uEWd2ZXJzaW9uAeABAXESICVo7+uNps8vj97ZCXGSjHMILC/r2ETg/dV3Gm/t/q4RpmNkaWR4IGRpZDpwbGM6anEzenZyYjVld2cycWd1cDczcXNvdXplY3Jldm0zbWdkeWNmdWIyeTIyY3NpZ1hAwKfrZtwwbN7dW0uSbviOs65NWQRvlS9Qc7oRtiorybMTEYxKGJaFK2kHIMEWIJqumb4751En2aJEpsilWlaQOWRkYXRh2CpYJQABcRIgnf7+Yd126j3K5QI4gLCDedV63yBILW/b4nWSifZHZ3tkcHJldvZndmVyc2lvbgMrAXESIJ3+/mHdduo9yuUCOICwg3nVet8gSC1v2+J1kon2R2d7omFlgGFs9mZjb21taXTYKlglAAFxEiAlaO/rjabPL4/e2QlxkoxzCCwv69hE4P3Vdxpv7f6uEWZyZWJhc2X0ZnRvb0JpZ/Q=";
        let bytes = data_encoding::BASE64.decode(FRAME).unwrap();
        let msg = decode_frame(&bytes).unwrap();
        let SubscribeReposMessage::Commit(c) = msg else {
            panic!("expected Commit");
        };
        assert!(c.since.is_none(), "since should be None for empty string");
    }

    #[cfg(feature = "relay")]
    #[test]
    fn test_decode_commit_missing_too_big_defaults_false() {
        const FRAME: &[u8] = b"omF0ZyNjb21taXRib3ABq2NvcHOAY3Jldm0zbWdkeWNmdWIyeTIyY3NlcRsAAAACZ8sHJmRyZXBveCBkaWQ6cGxjOmpxM3p2cmI1ZXdnMnFndXA3M3Fzb3V6ZWR0aW1leBsyMDI2LTAzLTA1VDE1OjQ3OjU0LjcxNjMyOFplYmxvYnOAZXNpbmNlYGZibG9ja3NZAUk6omVyb290c4HYKlglAAFxEiAlaO/rjabPL4/e2QlxkoxzCCwv69hE4P3Vdxpv7f6uEWd2ZXJzaW9uAeABAXESICVo7+uNps8vj97ZCXGSjHMILC/r2ETg/dV3Gm/t/q4RpmNkaWR4IGRpZDpwbGM6anEzenZyYjVld2cycWd1cDczcXNvdXplY3Jldm0zbWdkeWNmdWIyeTIyY3NpZ1hAwKfrZtwwbN7dW0uSbviOs65NWQRvlS9Qc7oRtiorybMTEYxKGJaFK2kHIMEWIJqumb4751En2aJEpsilWlaQOWRkYXRh2CpYJQABcRIgnf7+Yd126j3K5QI4gLCDedV63yBILW/b4nWSifZHZ3tkcHJldvZndmVyc2lvbgMrAXESIJ3+/mHdduo9yuUCOICwg3nVet8gSC1v2+J1kon2R2d7omFlgGFs9mZjb21taXTYKlglAAFxEiAlaO/rjabPL4/e2QlxkoxzCCwv69hE4P3Vdxpv7f6uEWZyZWJhc2X0ZnRvb0JpZ/Q=";
        let bytes = data_encoding::BASE64.decode(FRAME).unwrap();
        let SubscribeReposMessage::Commit(c) = decode_frame(&bytes).unwrap() else {
            panic!("expected Commit");
        };
        let frame = super::encode_frame(
            "#commit",
            &CommitWithoutTooBig {
                blobs: c.blobs.clone(),
                blocks: c.blocks.clone(),
                commit: c.commit.clone(),
                ops: c.ops.clone(),
                prev_data: c.prev_data.clone(),
                rebase: c.rebase,
                repo: c.repo.clone(),
                rev: c.rev,
                seq: c.seq,
                since: c.since,
                time: c.time,
            },
        )
        .unwrap();

        let SubscribeReposMessage::Commit(c) = decode_frame(&frame).unwrap() else {
            panic!("expected Commit");
        };
        assert!(!c.too_big, "missing tooBig should default to false");
    }

    #[cfg(feature = "relay")]
    #[test]
    fn test_decode_encoded_error_frame() {
        let bytes = super::encode_error_frame("ConsumerTooSlow", Some("blocked")).unwrap();
        let Err(FirehoseError::RelayError { error, message }) = decode_frame(&bytes) else {
            panic!("expected relay error");
        };

        assert_eq!(error, "ConsumerTooSlow");
        assert_eq!(message.as_deref(), Some("blocked"));
    }
}
