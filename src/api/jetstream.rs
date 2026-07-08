use std::time::Duration;

use axum::extract::{Query, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum_tws::{Message, WebSocket, WebSocketUpgrade};
use futures::{SinkExt, StreamExt};
use serde::{Deserialize, Deserializer};
use tokio::time::{MissedTickBehavior, interval, timeout};
use tracing::{debug, warn};

use crate::control::{Hydrant, JetstreamFilter, JetstreamSubscriberOptions};

const PING_INTERVAL: Duration = Duration::from_secs(30);
const CLOSE_TIMEOUT: Duration = Duration::from_secs(1);
const MAX_SUBSCRIBER_MESSAGE_BYTES: usize = 10_000_000;

// serde_urlencoded (used by axum's query extractor) cannot deserialize a single
// query param value into Vec<String> — it expects repeated keys for sequences.
// this visitor accepts both a bare string (single value) and a proper sequence.
fn deserialize_string_or_seq<'de, D>(deserializer: D) -> Result<Vec<String>, D::Error>
where
    D: Deserializer<'de>,
{
    struct StringOrSeq;

    impl<'de> serde::de::Visitor<'de> for StringOrSeq {
        type Value = Vec<String>;

        fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
            write!(f, "a string or sequence of strings")
        }

        fn visit_str<E: serde::de::Error>(self, v: &str) -> Result<Vec<String>, E> {
            Ok(vec![v.to_owned()])
        }

        fn visit_seq<A: serde::de::SeqAccess<'de>>(
            self,
            mut seq: A,
        ) -> Result<Vec<String>, A::Error> {
            let mut out = Vec::new();
            while let Some(v) = seq.next_element()? {
                out.push(v);
            }
            Ok(out)
        }
    }

    deserializer.deserialize_any(StringOrSeq)
}

#[derive(Deserialize)]
pub struct JetstreamQuery {
    #[serde(
        default,
        rename = "wantedCollections",
        deserialize_with = "deserialize_string_or_seq"
    )]
    wanted_collections: Vec<String>,
    #[serde(
        default,
        rename = "wantedDids",
        deserialize_with = "deserialize_string_or_seq"
    )]
    wanted_dids: Vec<String>,
    #[serde(default, rename = "maxMessageSizeBytes", alias = "maxSize")]
    max_message_size_bytes: Option<i64>,
    cursor: Option<i64>,
    #[serde(default)]
    compress: bool,
    #[serde(default, rename = "requireHello")]
    require_hello: bool,
    #[serde(
        default,
        rename = "wantedEventTypes",
        deserialize_with = "deserialize_string_or_seq"
    )]
    wanted_event_types: Vec<String>,
}

pub async fn handle_subscribe(
    State(hydrant): State<Hydrant>,
    Query(query): Query<JetstreamQuery>,
    headers: HeaderMap,
    ws: WebSocketUpgrade,
) -> Response {
    let options = match options_from_parts(
        &query.wanted_collections,
        &query.wanted_dids,
        parse_max_message_size(query.max_message_size_bytes),
        &query.wanted_event_types,
    ) {
        Ok(options) => JetstreamFilter::new(options),
        Err(err) => return (StatusCode::BAD_REQUEST, err).into_response(),
    };

    let socket_encoding = headers
        .get("Socket-Encoding")
        .and_then(|v| v.to_str().ok())
        .unwrap_or_default();
    let compress = query.compress || socket_encoding.contains("zstd");
    let cursor = query
        .cursor
        .filter(|cursor| *cursor <= chrono::Utc::now().timestamp_micros());

    ws.on_upgrade(move |socket| {
        handle_socket(
            socket,
            hydrant,
            cursor,
            options,
            compress,
            query.require_hello,
        )
    })
    .into_response()
}

async fn handle_socket(
    socket: WebSocket,
    hydrant: Hydrant,
    cursor: Option<i64>,
    options: JetstreamFilter,
    compress: bool,
    require_hello: bool,
) {
    let send_timeout = hydrant.stream_send_timeout();
    let (mut sink, mut ws_recv) = socket.split();

    if require_hello {
        loop {
            match ws_recv.next().await {
                Some(Ok(m)) if m.is_ping() => {
                    if sink.send(Message::pong(m.into_payload())).await.is_err() {
                        return;
                    }
                }
                Some(Ok(m)) if m.is_close() => return,
                Some(Ok(m)) if m.is_text() => match handle_options_message(m, &options) {
                    Ok(true) => break,
                    Ok(false) => return,
                    Err(_) => return,
                },
                Some(Ok(_)) => {}
                Some(Err(err)) => {
                    warn!(err = %err, "Jetstream ws recv error while waiting for hello");
                    return;
                }
                None => return,
            }
        }
    }

    let mut events = hydrant.subscribe_jetstream(cursor, options.clone());
    let mut ping_timer = interval(PING_INTERVAL);
    ping_timer.set_missed_tick_behavior(MissedTickBehavior::Delay);
    ping_timer.tick().await;

    loop {
        tokio::select! {
            inbound = ws_recv.next() => match inbound {
                Some(Ok(m)) if m.is_close() => break,
                Some(Ok(m)) if m.is_ping() => {
                    if let Err(err) = sink.send(Message::pong(m.into_payload())).await {
                        warn!(err = %err, "Jetstream ws pong send error");
                        break;
                    }
                }
                Some(Ok(m)) if m.is_text() => {
                    if let Err(err) = handle_options_message(m, &options) {
                        debug!(err = %err, "invalid Jetstream options update");
                        break;
                    }
                }
                Some(Ok(m)) if m.is_binary() => {
                    debug!("Jetstream client sent binary frame, closing");
                    break;
                }
                Some(Ok(_)) => {}
                Some(Err(err)) => {
                    warn!(err = %err, "Jetstream ws recv error");
                    break;
                }
                None => break,
            },
            evt = events.next() => match evt {
                Some(Ok(json)) => {
                    let msg = match frame_for_event(&json, compress, &options) {
                        Ok(Some(msg)) => msg,
                        Ok(None) => continue,
                        Err(err) => {
                            warn!(err = %err, "failed to encode Jetstream frame");
                            break;
                        }
                    };
                    match timeout(send_timeout, sink.send(msg)).await {
                        Ok(Ok(())) => {}
                        Ok(Err(err)) => {
                            warn!(err = %err, "Jetstream ws send error");
                            break;
                        }
                        Err(_) => {
                            let _ = timeout(
                                CLOSE_TIMEOUT,
                                sink.send(error_message("ConsumerTooSlow", &format!(
                                    "Jetstream socket send blocked for at least {} seconds",
                                    send_timeout.as_secs()
                                ))),
                            )
                            .await;
                            break;
                        }
                    }
                }
                Some(Err(err)) => {
                    let _ = timeout(send_timeout, sink.send(error_message(err.code(), &err.to_string()))).await;
                    break;
                }
                None => break,
            },
            _ = ping_timer.tick() => {
                if let Err(err) = sink.send(Message::ping(bytes::Bytes::new())).await {
                    warn!(err = %err, "Jetstream ws ping send error");
                    break;
                }
            }
        }
    }

    let _ = timeout(CLOSE_TIMEOUT, sink.close()).await;
}

fn handle_options_message(msg: Message, options: &JetstreamFilter) -> Result<bool, String> {
    let payload: bytes::Bytes = msg.into_payload().into();
    if payload.len() > MAX_SUBSCRIBER_MESSAGE_BYTES {
        return Err("subscriber message too large".into());
    }
    let msg: SubscriberSourcedMessage =
        serde_json::from_slice(&payload).map_err(|e| e.to_string())?;
    if msg.kind != "options_update" {
        return Ok(false);
    }
    let payload: SubscriberOptionsUpdatePayload =
        serde_json::from_value(msg.payload).map_err(|e| e.to_string())?;
    let next = options_from_parts(
        &payload.wanted_collections,
        &payload.wanted_dids,
        parse_max_message_size(Some(payload.max_message_size_bytes)),
        &payload.wanted_event_types,
    )?;
    options.update(next);
    Ok(true)
}

thread_local! {
    static ZSTD_COMPRESSOR: std::cell::RefCell<zstd::bulk::Compressor<'static>> = std::cell::RefCell::new(
        zstd::bulk::Compressor::new(3).expect("failed to initialize zstd compressor")
    );
}

fn frame_for_event(
    json: &[u8],
    compress: bool,
    options: &JetstreamFilter,
) -> Result<Option<Message>, String> {
    if exceeds_max(json.len(), options) {
        return Ok(None);
    }

    if compress {
        let compressed = ZSTD_COMPRESSOR
            .with(|c| c.borrow_mut().compress(json))
            .map_err(|e| e.to_string())?;

        if exceeds_max(compressed.len(), options) {
            return Ok(None);
        }
        return Ok(Some(Message::binary(compressed)));
    }

    let text = std::str::from_utf8(json).map_err(|e| e.to_string())?;
    Ok(Some(Message::text(text.to_string())))
}

fn exceeds_max(size: usize, options: &JetstreamFilter) -> bool {
    let max = options.max_message_size_bytes();
    max > 0 && size > max as usize
}

fn options_from_parts(
    wanted_collections: &[String],
    wanted_dids: &[String],
    max_message_size_bytes: u32,
    wanted_event_types: &[String],
) -> Result<JetstreamSubscriberOptions, String> {
    JetstreamSubscriberOptions::parse(
        wanted_collections,
        wanted_dids,
        max_message_size_bytes,
        wanted_event_types,
    )
}

fn parse_max_message_size(value: Option<i64>) -> u32 {
    value
        .filter(|v| *v > 0)
        .and_then(|v| u32::try_from(v).ok())
        .unwrap_or(0)
}

fn error_message(code: &str, message: &str) -> Message {
    Message::text(
        serde_json::json!({
            "type": "error",
            "error": code,
            "message": message,
        })
        .to_string(),
    )
}

#[derive(Deserialize)]
struct SubscriberSourcedMessage {
    #[serde(rename = "type")]
    kind: String,
    payload: serde_json::Value,
}

#[derive(Deserialize)]
struct SubscriberOptionsUpdatePayload {
    #[serde(default, rename = "wantedCollections")]
    wanted_collections: Vec<String>,
    #[serde(default, rename = "wantedDids")]
    wanted_dids: Vec<String>,
    #[serde(default, rename = "maxMessageSizeBytes", alias = "maxSize")]
    max_message_size_bytes: i64,
    #[serde(default, rename = "wantedEventTypes")]
    wanted_event_types: Vec<String>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_frame_for_event_size_limits() {
        let json = b"{\"hello\": \"world\"}";
        // options with max size 5
        let options =
            JetstreamFilter::new(JetstreamSubscriberOptions::parse(&[], &[], 5, &[]).unwrap());

        // Uncompressed exceeds limit
        let res = frame_for_event(json, false, &options).unwrap();
        assert!(res.is_none());

        // Compressed with uncompressed exceeding limit
        let res = frame_for_event(json, true, &options).unwrap();
        assert!(res.is_none());

        // Fits within limit
        let options_large =
            JetstreamFilter::new(JetstreamSubscriberOptions::parse(&[], &[], 100, &[]).unwrap());
        let res_uncompressed = frame_for_event(json, false, &options_large)
            .unwrap()
            .unwrap();
        assert!(res_uncompressed.is_text());

        let res_compressed = frame_for_event(json, true, &options_large)
            .unwrap()
            .unwrap();
        assert!(res_compressed.is_binary());
    }
}
