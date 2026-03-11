use crate::api::AppState;
use crate::db::keys;
use crate::types::{RepoState, ResyncState, StoredEvent};
use axum::routing::{get, post};
use axum::{
    Json,
    extract::{Query, State},
    http::StatusCode,
};
use jacquard_common::types::cid::Cid;
use jacquard_common::types::ident::AtIdentifier;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::str::FromStr;
use std::sync::Arc;

#[derive(Deserialize)]
pub struct DebugCountRequest {
    pub did: String,
    pub collection: String,
}

#[derive(Serialize)]
pub struct DebugCountResponse {
    pub count: usize,
}

pub fn router() -> axum::Router<Arc<AppState>> {
    axum::Router::new()
        .route("/debug/count", get(handle_debug_count))
        .route("/debug/get", get(handle_debug_get))
        .route("/debug/iter", get(handle_debug_iter))
        .route("/debug/refcount", get(handle_debug_refcount))
        .route("/debug/refcount", post(handle_set_debug_refcount))
        .route("/debug/repo_refcounts", get(handle_debug_repo_refcounts))
        .route("/debug/compact", post(handle_debug_compact))
}

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
                    match cid::Cid::read_bytes(k.as_ref()) {
                        Ok(cid) => cid.to_string(),
                        Err(_) => String::from_utf8_lossy(&k).into_owned(),
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
        "blocks" => Ok(db.blocks.clone()),
        "cursors" => Ok(db.cursors.clone()),
        "pending" => Ok(db.pending.clone()),
        "resync" => Ok(db.resync.clone()),
        "events" => Ok(db.events.clone()),
        "counts" => Ok(db.counts.clone()),
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
        let _ = ks.remove(b"dummy_tombstone123");
        let _ = state_clone.db.persist();
        let _ = ks.rotate_memtable_and_wait();
        ks.major_compact()
    })
    .await
    .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
    .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    Ok(StatusCode::OK)
}

#[derive(Deserialize)]
pub struct DebugRefcountRequest {
    pub cid: String,
}

#[derive(Serialize)]
pub struct DebugRefcountResponse {
    pub count: Option<i64>,
}

pub async fn handle_debug_refcount(
    State(state): State<Arc<AppState>>,
    Query(req): Query<DebugRefcountRequest>,
) -> Result<Json<DebugRefcountResponse>, StatusCode> {
    let cid = cid::Cid::from_str(&req.cid).map_err(|_| StatusCode::BAD_REQUEST)?;
    let cid_bytes = fjall::Slice::from(cid.to_bytes());

    let count = state
        .db
        .block_refcounts
        .read_sync(cid_bytes.as_ref(), |_, v| *v);

    Ok(Json(DebugRefcountResponse { count }))
}

#[derive(Deserialize)]
pub struct DebugSetRefcountRequest {
    pub cid: String,
    pub count: i64,
}

pub async fn handle_set_debug_refcount(
    State(state): State<Arc<AppState>>,
    axum::extract::Json(req): axum::extract::Json<DebugSetRefcountRequest>,
) -> Result<StatusCode, StatusCode> {
    let cid = cid::Cid::from_str(&req.cid).map_err(|_| StatusCode::BAD_REQUEST)?;
    let cid_bytes = fjall::Slice::from(cid.to_bytes());

    let _ = state.db.block_refcounts.insert_sync(cid_bytes, req.count);

    Ok(StatusCode::OK)
}

#[derive(Deserialize)]
pub struct DebugRepoRefcountsRequest {
    pub did: String,
}

#[derive(Serialize)]
pub struct DebugRepoRefcountsResponse {
    pub cids: std::collections::HashMap<String, i64>,
}

pub async fn handle_debug_repo_refcounts(
    State(state): State<Arc<AppState>>,
    Query(req): Query<DebugRepoRefcountsRequest>,
) -> Result<Json<DebugRepoRefcountsResponse>, StatusCode> {
    let raw_did = jacquard_common::types::ident::AtIdentifier::new(req.did.as_str())
        .map_err(|_| StatusCode::BAD_REQUEST)?;
    let did = state
        .resolver
        .resolve_did(&raw_did)
        .await
        .map_err(|_| StatusCode::BAD_REQUEST)?;

    let state_clone = state.clone();

    let cids = tokio::task::spawn_blocking(move || {
        let mut unique_cids: std::collections::HashSet<String> = std::collections::HashSet::new();
        let db = &state_clone.db;
        
        // 1. Scan records
        let records_prefix = crate::db::keys::record_prefix_did(&did);
        for guard in db.records.prefix(&records_prefix) {
            if let Ok((_k, v)) = guard.into_inner() {
                if let Ok(cid) = cid::Cid::read_bytes(v.as_ref()) {
                    unique_cids.insert(cid.to_string());
                }
            }
        }
        
        // 2. Scan events
        let trimmed_did = crate::db::types::TrimmedDid::from(&did);
        for guard in db.events.iter() {
            if let Ok((_k, v)) = guard.into_inner() {
                if let Ok(evt) = rmp_serde::from_slice::<crate::types::StoredEvent>(v.as_ref()) {
                    if evt.did == trimmed_did {
                        if let Some(cid) = evt.cid {
                            unique_cids.insert(cid.to_string());
                        }
                    }
                }
            }
        }
        
        let mut counts: std::collections::HashMap<String, i64> = std::collections::HashMap::new();
        for cid_str in unique_cids {
            if let Ok(cid) = cid::Cid::from_str(&cid_str) {
                let cid_bytes = fjall::Slice::from(cid.to_bytes());
                let count = db.block_refcounts.read_sync(cid_bytes.as_ref(), |_, v| *v).unwrap_or(0);
                counts.insert(cid_str, count);
            }
        }
        counts
    })
    .await
    .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    Ok(Json(DebugRepoRefcountsResponse { cids }))
}
