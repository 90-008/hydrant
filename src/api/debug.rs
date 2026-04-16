use crate::api::AppState;
use crate::db::keys;
#[cfg(feature = "indexer_stream")]
use crate::types::StoredEvent;
use crate::types::{RepoState, ResyncState};
use axum::routing::{get, post};
use axum::{
    Json,
    extract::{Query, State},
    http::StatusCode,
};
use jacquard_common::types::cid::Cid;
#[cfg(feature = "indexer")]
use jacquard_common::types::ident::AtIdentifier;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::str::FromStr;
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
        .route("/debug/seed_watermark", post(handle_debug_seed_watermark));

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

    let db = &state.db;
    let ks = db.records.clone();

    // {TrimmedDid}|{collection}|
    let prefix = keys::record_prefix_collection(&did, &req.collection);

    let count = tokio::task::spawn_blocking(move || {
        let start_key = prefix.clone();
        let mut end_key = prefix.clone();
        if let Some(msg) = end_key.last_mut() {
            *msg += 1;
        }

        ks.range(start_key..end_key).count()
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

fn deserialize_value(partition: &str, value: &[u8]) -> Value {
    match partition {
        "repos" => {
            if let Ok(state) = rmp_serde::from_slice::<RepoState>(value) {
                return serde_json::to_value(state).unwrap_or(Value::Null);
            }
        }
        "resync" => {
            if let Ok(state) = rmp_serde::from_slice::<ResyncState>(value) {
                return serde_json::to_value(state).unwrap_or(Value::Null);
            }
        }
        #[cfg(feature = "indexer_stream")]
        "events" => {
            if let Ok(event) = rmp_serde::from_slice::<StoredEvent>(value) {
                return serde_json::to_value(event).unwrap_or(Value::Null);
            }
        }
        "records" => {
            if let Ok(s) = String::from_utf8(value.to_vec()) {
                match Cid::from_str(&s) {
                    Ok(cid) => return serde_json::to_value(cid).unwrap_or(Value::String(s)),
                    Err(_) => return Value::String(s),
                }
            }
        }
        "counts" | "cursors" => {
            if let Ok(arr) = value.try_into() {
                return Value::Number(u64::from_be_bytes(arr).into());
            }
            if let Ok(s) = String::from_utf8(value.to_vec()) {
                return Value::String(s);
            }
        }
        "blocks" => {
            if let Ok(val) = serde_ipld_dagcbor::from_slice::<Value>(value) {
                return val;
            }
        }
        "pending" => return Value::Null,
        _ => {}
    }
    Value::String(hex::encode(value))
}

pub async fn handle_debug_get(
    State(state): State<Arc<AppState>>,
    Query(req): Query<DebugGetRequest>,
) -> Result<Json<DebugGetResponse>, StatusCode> {
    let ks = get_keyspace_by_name(&state.db, &req.partition)?;

    let key = if req.partition == "events" {
        let id = req
            .key
            .parse::<u64>()
            .map_err(|_| StatusCode::BAD_REQUEST)?;
        id.to_be_bytes().to_vec()
    } else {
        req.key.into_bytes()
    };

    let partition = req.partition.clone();
    let value = crate::db::Db::get(ks, key)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
        .map(|v| deserialize_value(&partition, &v));

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
    let ks = get_keyspace_by_name(&state.db, &req.partition)?;
    let is_events = req.partition == "events";
    let partition = req.partition.clone();

    let parse_bound = |s: Option<String>| -> Result<Option<Vec<u8>>, StatusCode> {
        match s {
            Some(s) => {
                if is_events {
                    let id = s.parse::<u64>().map_err(|_| StatusCode::BAD_REQUEST)?;
                    Ok(Some(id.to_be_bytes().to_vec()))
                } else {
                    Ok(Some(s.into_bytes()))
                }
            }
            None => Ok(None),
        }
    };

    let start = parse_bound(req.start)?;
    let end = parse_bound(req.end)?;

    let items = tokio::task::spawn_blocking(move || {
        let limit = req.limit.unwrap_or(50);

        let collect = |iter: &mut dyn Iterator<Item = fjall::Guard>| {
            let mut items = Vec::new();
            for guard in iter.take(limit) {
                let (k, v) = guard
                    .into_inner()
                    .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

                let key_str = if is_events {
                    if let Ok(arr) = k.as_ref().try_into() {
                        u64::from_be_bytes(arr).to_string()
                    } else {
                        "invalid_u64".to_string()
                    }
                } else if partition == "blocks" {
                    // key is col|cid_bytes, show as "col|<cid_str>"
                    if let Some(sep) = k.iter().position(|&b| b == keys::SEP) {
                        let col = String::from_utf8_lossy(&k[..sep]);
                        match cid::Cid::read_bytes(&k[sep + 1..]) {
                            Ok(cid) => format!("{col}|{cid}"),
                            Err(_) => String::from_utf8_lossy(&k).into_owned(),
                        }
                    } else {
                        String::from_utf8_lossy(&k).into_owned()
                    }
                } else {
                    String::from_utf8_lossy(&k).into_owned()
                };

                items.push((key_str, deserialize_value(&partition, &v)));
            }
            Ok::<_, StatusCode>(items)
        };

        let start_bound = if let Some(ref s) = start {
            std::ops::Bound::Included(s.as_slice())
        } else {
            std::ops::Bound::Unbounded
        };

        let end_bound = if let Some(ref e) = end {
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
    .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)??;

    Ok(Json(DebugIterResponse { items }))
}

fn get_keyspace_by_name(db: &crate::db::Db, name: &str) -> Result<fjall::Keyspace, StatusCode> {
    match name {
        "repos" => Ok(db.repos.clone()),
        "counts" => Ok(db.counts.clone()),
        "cursors" => Ok(db.cursors.clone()),
        #[cfg(feature = "indexer")]
        "blocks" => Ok(db.blocks.clone()),
        #[cfg(feature = "indexer")]
        "pending" => Ok(db.pending.clone()),
        #[cfg(feature = "indexer")]
        "resync" => Ok(db.resync.clone()),
        #[cfg(feature = "indexer_stream")]
        "events" => Ok(db.events.clone()),
        #[cfg(feature = "indexer")]
        "records" => Ok(db.records.clone()),
        _ => Err(StatusCode::BAD_REQUEST),
    }
}

#[derive(Deserialize)]
pub struct DebugCompactRequest {
    pub partition: String,
}

pub async fn handle_debug_compact(
    State(state): State<Arc<AppState>>,
    Query(req): Query<DebugCompactRequest>,
) -> Result<StatusCode, StatusCode> {
    let ks = get_keyspace_by_name(&state.db, &req.partition)?;
    let state_clone = state.clone();

    tokio::task::spawn_blocking(move || {
        ks.remove(b"dummy_tombstone123")?;
        state_clone.db.inner.persist(fjall::PersistMode::Buffer)?;
        ks.rotate_memtable_and_wait()?;
        ks.major_compact()
    })
    .await
    .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
    .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    Ok(StatusCode::OK)
}

#[cfg(any(feature = "indexer_stream", feature = "relay"))]
pub async fn handle_debug_ephemeral_ttl_tick(
    State(state): State<Arc<AppState>>,
) -> Result<StatusCode, StatusCode> {
    tokio::task::spawn_blocking(move || -> miette::Result<()> {
        #[cfg(feature = "indexer_stream")]
        crate::db::ephemeral::ephemeral_ttl_tick(&state.db, &state.ephemeral_ttl)?;
        #[cfg(feature = "relay")]
        crate::db::ephemeral::relay_events_ttl_tick(&state.db, &state.ephemeral_ttl)?;
        Ok(())
    })
    .await
    .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
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
    tokio::task::spawn_blocking(move || -> Result<(), StatusCode> {
        #[cfg(feature = "indexer_stream")]
        state
            .db
            .cursors
            .insert(
                crate::db::keys::event_watermark_key(req.ts),
                req.event_id.to_be_bytes(),
            )
            .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
        #[cfg(feature = "relay")]
        state
            .db
            .cursors
            .insert(
                crate::db::keys::relay_event_watermark_key(req.ts),
                req.event_id.to_be_bytes(),
            )
            .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
        Ok(())
    })
    .await
    .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)??;

    Ok(StatusCode::OK)
}
