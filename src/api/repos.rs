use std::sync::Arc;

use axum::{
    Router,
    body::Body,
    extract::{Query, State},
    http::{StatusCode, header},
    response::{IntoResponse, Response},
    routing::{delete, get, put},
};
use jacquard::{IntoStatic, types::did::Did};
use miette::IntoDiagnostic;
use rand::Rng;
use serde::{Deserialize, Serialize};

use crate::api::AppState;
use crate::db::{keys, ser_repo_state};
use crate::types::{GaugeState, RepoState};

pub fn router() -> Router<Arc<AppState>> {
    Router::new()
        .route("/repos", get(handle_get_repos))
        .route("/repos", put(handle_put_repos))
        .route("/repos", delete(handle_delete_repos))
}

#[derive(Deserialize, Debug)]
pub struct RepoRequest {
    pub did: String,
    #[serde(skip_serializing_if = "Option::is_none", rename = "deleteData")]
    pub delete_data: Option<bool>,
}

#[derive(Serialize, Debug)]
pub struct RepoResponse {
    pub did: String,
    pub status: String,
    pub tracked: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub rev: Option<String>,
    pub last_updated_at: i64,
}

#[derive(Deserialize)]
pub struct DeleteParams {
    #[serde(default)]
    pub delete_data: bool,
}

#[derive(Deserialize)]
pub struct GetReposParams {
    pub limit: Option<usize>,
    pub cursor: Option<String>,
    pub partition: Option<String>,
}

pub async fn handle_get_repos(
    State(state): State<Arc<AppState>>,
    Query(params): Query<GetReposParams>,
) -> Result<Response, (StatusCode, String)> {
    let limit = params.limit.unwrap_or(100).min(1000);
    let partition = params.partition.unwrap_or_else(|| "all".to_string());

    let items = tokio::task::spawn_blocking(move || {
        let db = &state.db;

        let to_response = |k: &[u8], v: &[u8]| -> Result<RepoResponse, (StatusCode, String)> {
            let repo_state = crate::db::deser_repo_state(v).map_err(internal)?;
            let did = crate::db::types::TrimmedDid::try_from(k)
                .map_err(internal)?
                .to_did();

            Ok(RepoResponse {
                did: did.to_string(),
                status: repo_state.status.to_string(),
                tracked: repo_state.tracked,
                rev: repo_state.rev.as_ref().map(|r| r.to_string()),
                last_updated_at: repo_state.last_updated_at,
            })
        };

        let results = match partition.as_str() {
            "all" | "resync" => {
                let is_all = partition == "all";
                let ks = if is_all { &db.repos } else { &db.resync };

                let start_bound = if let Some(cursor) = params.cursor {
                    let did = Did::new_owned(&cursor).map_err(bad_request)?;
                    let did_key = keys::repo_key(&did);
                    std::ops::Bound::Excluded(did_key)
                } else {
                    std::ops::Bound::Unbounded
                };

                let mut items = Vec::new();
                for item in ks
                    .range((start_bound, std::ops::Bound::Unbounded))
                    .take(limit)
                {
                    let (k, v) = item.into_inner().map_err(internal)?;

                    let repo_state_bytes = if is_all {
                        v
                    } else {
                        db.repos.get(&k).map_err(internal)?.ok_or_else(|| {
                            internal(format!("repository state missing for {}", partition))
                        })?
                    };

                    items.push(to_response(&k, &repo_state_bytes)?);
                }
                Ok::<_, (StatusCode, String)>(items)
            }
            "pending" => {
                let start_bound = if let Some(cursor) = params.cursor {
                    let id = cursor.parse::<u64>().map_err(bad_request)?;
                    std::ops::Bound::Excluded(id.to_be_bytes().to_vec())
                } else {
                    std::ops::Bound::Unbounded
                };

                let mut items = Vec::new();
                for item in db
                    .pending
                    .range((start_bound, std::ops::Bound::Unbounded))
                    .take(limit)
                {
                    let (_, did_key) = item.into_inner().map_err(internal)?;

                    if let Ok(Some(v)) = db.repos.get(&did_key) {
                        items.push(to_response(&did_key, &v)?);
                    }
                }
                Ok(items)
            }
            _ => Err((StatusCode::BAD_REQUEST, "invalid partition".to_string())),
        }?;

        Ok::<_, (StatusCode, String)>(results)
    })
    .await
    .map_err(internal)??;

    use futures::StreamExt;

    let stream = futures::stream::iter(items.into_iter().map(|item| {
        let json = serde_json::to_string(&item).ok()?;
        Some(Ok::<_, std::io::Error>(format!("{json}\n")))
    }))
    .filter_map(|x| futures::future::ready(x));

    let body = Body::from_stream(stream);

    Ok(([(header::CONTENT_TYPE, "application/x-ndjson")], body).into_response())
}

pub async fn handle_put_repos(
    State(state): State<Arc<AppState>>,
    req: axum::extract::Request,
) -> Result<StatusCode, (StatusCode, String)> {
    let items = parse_body(req).await?;

    let state_task = state.clone();
    let (new_repo_count, gauge_transitions) = tokio::task::spawn_blocking(move || {
        let db = &state_task.db;
        let mut batch = db.inner.batch();
        let mut added = 0i64;
        let mut gauge_transitions: Vec<(GaugeState, GaugeState)> = Vec::new();

        let mut rng = rand::rng();

        for item in items {
            let did = Did::new(&item.did).map_err(bad_request)?;
            let did_key = keys::repo_key(&did);

            let repo_bytes = db.repos.get(&did_key).map_err(internal)?;
            let existing_state = repo_bytes
                .as_deref()
                .map(crate::db::deser_repo_state)
                .transpose()
                .map_err(internal)?;

            if let Some(mut repo_state) = existing_state {
                if !repo_state.tracked {
                    let resync_bytes = db.resync.get(&did_key).map_err(internal)?;
                    let old_gauge =
                        crate::db::Db::repo_gauge_state(&repo_state, resync_bytes.as_deref());

                    repo_state.tracked = true;
                    // re-enqueue into pending
                    batch.insert(
                        &db.repos,
                        &did_key,
                        ser_repo_state(&repo_state).map_err(internal)?,
                    );
                    batch.insert(
                        &db.pending,
                        keys::pending_key(repo_state.index_id),
                        &did_key,
                    );
                    batch.remove(&db.resync, &did_key);
                    gauge_transitions.push((old_gauge, GaugeState::Pending));
                }
            } else {
                let repo_state = RepoState::backfilling(rng.next_u64());
                batch.insert(
                    &db.repos,
                    &did_key,
                    ser_repo_state(&repo_state).map_err(internal)?,
                );
                batch.insert(
                    &db.pending,
                    keys::pending_key(repo_state.index_id),
                    &did_key,
                );
                added += 1;
                gauge_transitions.push((GaugeState::Synced, GaugeState::Pending)); // pseudo-transition to just inc pending
            }
        }

        batch.commit().into_diagnostic().map_err(internal)?;

        Ok::<_, (StatusCode, String)>((added, gauge_transitions))
    })
    .await
    .map_err(internal)??;

    if new_repo_count > 0 {
        state.db.update_count_async("repos", new_repo_count).await;
    }
    for (old, new) in gauge_transitions {
        state.db.update_gauge_diff_async(&old, &new).await;
    }

    // Always notify backfill if anything was added to pending!
    state.notify_backfill();

    Ok(StatusCode::OK)
}

pub async fn handle_delete_repos(
    State(state): State<Arc<AppState>>,
    Query(params): Query<DeleteParams>,
    req: axum::extract::Request,
) -> Result<StatusCode, (StatusCode, String)> {
    let items = parse_body(req).await?;

    let state_task = state.clone();
    let (deleted_count, gauge_decrements) = tokio::task::spawn_blocking(move || {
        let db = &state_task.db;
        let mut batch = db.inner.batch();
        let mut deleted_count = 0i64;
        let mut gauge_decrements = Vec::new();

        for item in items {
            let did = Did::new(&item.did).map_err(bad_request)?;
            let delete_data = item.delete_data.unwrap_or(params.delete_data);
            let did_key = keys::repo_key(&did);

            let repo_bytes = db.repos.get(&did_key).map_err(internal)?;
            let existing_state = repo_bytes
                .as_deref()
                .map(crate::db::deser_repo_state)
                .transpose()
                .map_err(internal)?;

            if let Some(repo_state) = existing_state {
                let resync_bytes = db.resync.get(&did_key).map_err(internal)?;
                let old_gauge =
                    crate::db::Db::repo_gauge_state(&repo_state, resync_bytes.as_deref());

                if delete_data {
                    crate::ops::delete_repo(&mut batch, db, &did, &repo_state).map_err(internal)?;
                    deleted_count += 1;
                    if old_gauge != GaugeState::Synced {
                        gauge_decrements.push(old_gauge);
                    }
                } else if repo_state.tracked {
                    let mut repo_state = repo_state.into_static();
                    repo_state.tracked = false;
                    batch.insert(
                        &db.repos,
                        &did_key,
                        ser_repo_state(&repo_state).map_err(internal)?,
                    );
                    batch.remove(&db.pending, keys::pending_key(repo_state.index_id));
                    batch.remove(&db.resync, &did_key);
                    if old_gauge != GaugeState::Synced {
                        gauge_decrements.push(old_gauge);
                    }
                }
            }
        }

        batch.commit().into_diagnostic().map_err(internal)?;

        Ok::<_, (StatusCode, String)>((deleted_count, gauge_decrements))
    })
    .await
    .map_err(internal)??;

    if deleted_count > 0 {
        state.db.update_count_async("repos", -deleted_count).await;
    }
    for gauge in gauge_decrements {
        state
            .db
            .update_gauge_diff_async(&gauge, &GaugeState::Synced)
            .await;
    }

    Ok(StatusCode::OK)
}

async fn parse_body(req: axum::extract::Request) -> Result<Vec<RepoRequest>, (StatusCode, String)> {
    let content_type = req
        .headers()
        .get(header::CONTENT_TYPE)
        .and_then(|h| h.to_str().ok())
        .unwrap_or("")
        .to_string();

    let body_bytes = axum::body::to_bytes(req.into_body(), usize::MAX)
        .await
        .map_err(bad_request)?;

    let text = std::str::from_utf8(&body_bytes).map_err(bad_request)?;
    let trimmed = text.trim();

    if content_type.contains("application/json") {
        serde_json::from_str::<Vec<RepoRequest>>(trimmed)
            .map_err(|e| bad_request(format!("invalid JSON array: {e}")))
    } else {
        trimmed
            .lines()
            .filter(|l| !l.trim().is_empty())
            .map(|line| {
                serde_json::from_str::<RepoRequest>(line)
                    .map_err(|e| bad_request(format!("invalid NDJSON line: {e}")))
            })
            .collect()
    }
}

fn bad_request<E: std::fmt::Display>(err: E) -> (StatusCode, String) {
    (StatusCode::BAD_REQUEST, err.to_string())
}

fn internal<E: std::fmt::Display>(err: E) -> (StatusCode, String) {
    (StatusCode::INTERNAL_SERVER_ERROR, err.to_string())
}
