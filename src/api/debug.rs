use crate::api::AppState;
#[cfg(feature = "indexer")]
use crate::db::keys;
use crate::db::registry;
use axum::routing::{get, post};
use axum::{
    Json,
    extract::{Query, State},
    http::StatusCode,
};
#[cfg(feature = "indexer")]
use jacquard_common::types::ident::AtIdentifier;
use miette::IntoDiagnostic;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::sync::Arc;

#[cfg(feature = "indexer")]
#[derive(Deserialize)]
pub struct DebugCountRequest {
    pub did: String,
    pub collection: String,
}

#[cfg(feature = "indexer")]
#[derive(Serialize)]
pub struct DebugCountResponse {
    pub count: usize,
}

pub fn router() -> axum::Router<Arc<AppState>> {
    let r = axum::Router::new()
        .route("/debug/get", get(handle_debug_get))
        .route("/debug/iter", get(handle_debug_iter))
        .route("/debug/compact", post(handle_debug_compact));

    #[cfg(any(feature = "indexer_stream", feature = "relay"))]
    let r = r
        .route(
            "/debug/ephemeral_ttl_tick",
            post(handle_debug_ephemeral_ttl_tick),
        )
        .route("/debug/seed_watermark", post(handle_debug_seed_watermark))
        .route("/debug/seed_events", post(handle_debug_seed_events));

    #[cfg(feature = "indexer")]
    let r = r.route("/debug/count", get(handle_debug_count));

    r
}

#[cfg(feature = "indexer")]
pub async fn handle_debug_count(
    State(state): State<Arc<AppState>>,
    Query(req): Query<DebugCountRequest>,
) -> Result<Json<DebugCountResponse>, StatusCode> {
    let did = state
        .resolver
        .resolve_did(&AtIdentifier::new(req.did.as_str()).map_err(|_| StatusCode::BAD_REQUEST)?)
        .await
        .map_err(|_| StatusCode::BAD_REQUEST)?;

    let prefix = keys::record_prefix_collection(&did, &req.collection);

    let count = state
        .db
        .run(move |db| {
            let ks = db
                .keyspace_by_name("records")
                .expect("records keyspace exists in indexer mode");

            let start_key = prefix.clone();
            let mut end_key = prefix.clone();
            if let Some(msg) = end_key.last_mut() {
                *msg += 1;
            }

            Ok::<_, miette::Report>(ks.range(start_key..end_key).count())
        })
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    Ok(Json(DebugCountResponse { count }))
}

#[derive(Deserialize)]
pub struct DebugGetRequest {
    pub partition: String,
    pub key: String,
}

#[derive(Serialize)]
pub struct DebugGetResponse {
    pub value: Option<Value>,
}

pub async fn handle_debug_get(
    State(state): State<Arc<AppState>>,
    Query(req): Query<DebugGetRequest>,
) -> Result<Json<DebugGetResponse>, StatusCode> {
    let ks = get_keyspace_by_name(&state.db, &req.partition)?;

    let key = registry::debug_parse_key(&req.partition, &req.key).ok_or(StatusCode::BAD_REQUEST)?;

    let partition = req.partition.clone();
    let value = state
        .db
        .run(move |_| {
            ks.get(key)
                .inspect_err(crate::db::check_poisoned)
                .into_diagnostic()
        })
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
        .map(|v| registry::debug_value(&partition, &v));

    Ok(Json(DebugGetResponse { value }))
}

#[derive(Deserialize)]
pub struct DebugIterRequest {
    pub partition: String,
    pub start: Option<String>,
    pub end: Option<String>,
    pub limit: Option<usize>,
    pub reverse: Option<bool>,
}

#[derive(Serialize)]
pub struct DebugIterResponse {
    pub items: Vec<(String, Value)>,
}

pub async fn handle_debug_iter(
    State(state): State<Arc<AppState>>,
    Query(req): Query<DebugIterRequest>,
) -> Result<Json<DebugIterResponse>, StatusCode> {
    let partition = req.partition.clone();

    let parse_bound = |s: Option<String>| -> Result<Option<Vec<u8>>, StatusCode> {
        s.map(|s| registry::debug_parse_key(&partition, &s).ok_or(StatusCode::BAD_REQUEST))
            .transpose()
    };

    let start = parse_bound(req.start)?;
    let end = parse_bound(req.end)?;

    let items = state
        .db
        .run(move |db| {
            let ks = get_keyspace_by_name(db, &req.partition)
                .map_err(|_| miette::miette!("bad request"))?;
            let limit = req.limit.unwrap_or(50);

            let collect = |iter: &mut dyn Iterator<Item = fjall::Guard>| {
                let mut items = Vec::new();
                for guard in iter.take(limit) {
                    let (k, v) = guard
                        .into_inner()
                        .map_err(|_| miette::miette!("internal error"))?;

                    let key_str = registry::debug_render_key(&partition, &k);

                    items.push((key_str, registry::debug_value(&partition, &v)));
                }
                Ok::<_, miette::Report>(items)
            };

            let start_bound = if let Some(s) = &start {
                std::ops::Bound::Included(s.as_slice())
            } else {
                std::ops::Bound::Unbounded
            };

            let end_bound = if let Some(e) = &end {
                std::ops::Bound::Included(e.as_slice())
            } else {
                std::ops::Bound::Unbounded
            };

            if req.reverse == Some(true) {
                collect(
                    &mut ks
                        .range::<&[u8], (std::ops::Bound<&[u8]>, std::ops::Bound<&[u8]>)>((
                            start_bound,
                            end_bound,
                        ))
                        .rev(),
                )
            } else {
                collect(
                    &mut ks.range::<&[u8], (std::ops::Bound<&[u8]>, std::ops::Bound<&[u8]>)>((
                        start_bound,
                        end_bound,
                    )),
                )
            }
        })
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    Ok(Json(DebugIterResponse { items }))
}

fn get_keyspace_by_name(db: &crate::db::Db, name: &str) -> Result<fjall::Keyspace, StatusCode> {
    db.keyspace_by_name(name).ok_or(StatusCode::BAD_REQUEST)
}

#[derive(Deserialize)]
pub struct DebugCompactRequest {
    pub partition: String,
}

pub async fn handle_debug_compact(
    State(state): State<Arc<AppState>>,
    Query(req): Query<DebugCompactRequest>,
) -> Result<StatusCode, StatusCode> {
    state
        .db
        .run(move |db| {
            let ks = get_keyspace_by_name(db, &req.partition)
                .map_err(|_| miette::miette!("bad request"))?;
            ks.remove(b"dummy_tombstone123").into_diagnostic()?;
            db.inner
                .persist(fjall::PersistMode::Buffer)
                .into_diagnostic()?;
            ks.rotate_memtable_and_wait().into_diagnostic()?;
            ks.major_compact().into_diagnostic()?;
            Ok::<_, miette::Report>(())
        })
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    Ok(StatusCode::OK)
}

#[cfg(any(feature = "indexer_stream", feature = "relay"))]
pub async fn handle_debug_ephemeral_ttl_tick(
    State(state): State<Arc<AppState>>,
) -> Result<StatusCode, StatusCode> {
    let ephemeral_ttl = state.ephemeral_ttl;
    state
        .db
        .run(move |db| {
            #[cfg(feature = "indexer_stream")]
            crate::db::ephemeral::ephemeral_ttl_tick(db, &ephemeral_ttl)?;
            #[cfg(feature = "relay")]
            crate::db::ephemeral::relay_events_ttl_tick(db, &ephemeral_ttl)?;
            #[cfg(feature = "jetstream")]
            crate::db::ephemeral::jetstream_events_ttl_tick(db, &ephemeral_ttl)?;
            Ok::<_, miette::Report>(())
        })
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    Ok(StatusCode::OK)
}

#[derive(Deserialize)]
#[cfg(any(feature = "indexer_stream", feature = "relay"))]
pub struct DebugSeedWatermarkRequest {
    /// unix timestamp (seconds) to write the watermark at
    pub ts: u64,
    /// event_id the watermark points to, all events before this id will be pruned
    pub event_id: u64,
}

/// writes an event watermark entry directly to the cursors keyspace, using identical
/// key/value encoding to the real TTL worker. used in tests to plant a past watermark
/// so the real `ephemeral_ttl_tick` code path is exercised without waiting 3600 seconds.
#[cfg(any(feature = "indexer_stream", feature = "relay"))]
pub async fn handle_debug_seed_watermark(
    State(state): State<Arc<AppState>>,
    Query(req): Query<DebugSeedWatermarkRequest>,
) -> Result<StatusCode, StatusCode> {
    state
        .db
        .run(move |db| {
            #[cfg(feature = "indexer_stream")]
            db.cursors
                .insert(
                    crate::db::keys::event_watermark_key(req.ts),
                    req.event_id.to_be_bytes(),
                )
                .into_diagnostic()?;
            #[cfg(feature = "relay")]
            db.cursors
                .insert(
                    crate::db::keys::relay_event_watermark_key(req.ts),
                    req.event_id.to_be_bytes(),
                )
                .into_diagnostic()?;
            Ok::<_, miette::Report>(())
        })
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    Ok(StatusCode::OK)
}

#[derive(Deserialize)]
#[cfg(any(feature = "indexer_stream", feature = "relay"))]
pub struct DebugSeedEventsRequest {
    pub partition: String,
    pub count: u64,
}

#[cfg(any(feature = "indexer_stream", feature = "relay"))]
pub async fn handle_debug_seed_events(
    State(state): State<Arc<AppState>>,
    Query(req): Query<DebugSeedEventsRequest>,
) -> Result<StatusCode, StatusCode> {
    if req.partition != "events" && req.partition != "relay_events" {
        return Err(StatusCode::BAD_REQUEST);
    }

    state
        .db
        .run(move |db| {
            let mut batch = db.inner.batch();
            if req.partition == "events" {
                #[cfg(feature = "indexer_stream")]
                {
                    for _ in 0..req.count {
                        let seq = db
                            .stream
                            .next_event_id
                            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                        db.stream.stage_event(
                            &mut batch,
                            crate::db::keys::event_key(seq),
                            b"dummy",
                        );
                    }
                }
            } else if req.partition == "relay_events" {
                #[cfg(feature = "relay")]
                {
                    for _ in 0..req.count {
                        let seq = db
                            .relay
                            .next_seq
                            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                        batch.insert(
                            &db.relay.events,
                            crate::db::keys::relay_event_key(seq),
                            b"dummy",
                        );
                    }
                }
            }
            batch.commit().into_diagnostic()?;
            Ok::<_, miette::Report>(())
        })
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    Ok(StatusCode::OK)
}
