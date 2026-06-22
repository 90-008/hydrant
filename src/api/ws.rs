use std::time::Duration;

use axum_tws::{Limits, Message, WebSocket};
use bytes::Bytes;
use futures::{SinkExt, Stream, StreamExt};
use tokio::time::{MissedTickBehavior, interval, timeout};
use tracing::{debug, warn};

const PING_INTERVAL: Duration = Duration::from_secs(30);
const CLOSE_TIMEOUT: Duration = Duration::from_secs(1);
const CONTROL_FRAME_PAYLOAD_LIMIT: usize = 125;

pub(super) fn control_frame_only_limits() -> Limits {
    Limits::default().max_payload_len(Some(CONTROL_FRAME_PAYLOAD_LIMIT))
}

pub(super) enum WsAction {
    Send(Message),
    #[cfg_attr(not(feature = "indexer_stream"), allow(dead_code))]
    Skip,
    Close(Option<Message>),
}

pub(super) async fn run_socket<S, F, G>(
    socket: WebSocket,
    mut events: S,
    mut to_action: F,
    send_timeout: Duration,
    slow_consumer: G,
) where
    S: Stream + Unpin,
    F: FnMut(S::Item) -> WsAction,
    G: FnOnce(Duration) -> Option<Message>,
{
    let (mut sink, mut ws_recv) = socket.split();

    let mut ping_timer = interval(PING_INTERVAL);
    ping_timer.set_missed_tick_behavior(MissedTickBehavior::Delay);
    ping_timer.tick().await;

    let mut slow_consumer = Some(slow_consumer);

    loop {
        tokio::select! {
            inbound = ws_recv.next() => match inbound {
                Some(Ok(m)) if m.is_close() => break,
                Some(Ok(m)) if m.is_text() || m.is_binary() => {
                    debug!("client sent unsolicited data frame, closing");
                    break;
                }
                Some(Ok(m)) if m.is_ping() => {
                    if let Err(err) = sink.send(Message::pong(m.into_payload())).await {
                        warn!(err = %err, "ws pong send error");
                        break;
                    }
                }
                Some(Ok(_)) => {}
                Some(Err(e)) => {
                    warn!(err = %e, "ws recv error");
                    break;
                }
                None => break,
            },
            evt = events.next() => match evt {
                Some(item) => match to_action(item) {
                    WsAction::Skip => {}
                    WsAction::Send(msg) => match timeout(send_timeout, sink.send(msg)).await {
                        Ok(Ok(())) => {}
                        Ok(Err(err)) => {
                            warn!(err = %err, "ws send error");
                            break;
                        }
                        Err(_) => {
                            if let Some(final_msg) =
                                slow_consumer.take().and_then(|f| f(send_timeout))
                            {
                                let _ = timeout(CLOSE_TIMEOUT, sink.send(final_msg)).await;
                            }
                            break;
                        }
                    },
                    WsAction::Close(final_msg) => {
                        if let Some(m) = final_msg {
                            let _ = timeout(send_timeout, sink.send(m)).await;
                        }
                        break;
                    }
                },
                None => break,
            },
            _ = ping_timer.tick() => {
                if let Err(err) = sink.send(Message::ping(Bytes::new())).await {
                    warn!(err = %err, "ws ping send error");
                    break;
                }
            }
        }
    }
    let _ = timeout(CLOSE_TIMEOUT, sink.close()).await;
}
