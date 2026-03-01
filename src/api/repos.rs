use std::sync::Arc;

use axum::{
    Router,
    body::Body,
    extract::{Query, State},
    http::{StatusCode, header},
    response::{IntoResponse, Response},
    routing::{delete, get, put},
};
use jacquard::IntoStatic;
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

        let results = match partition.as_str() {
            "all" => {
                let start_bound = if let Some(cursor) = params.cursor {
                    let did = jacquard::types::did::Did::new_owned(&cursor)
                        .map_err(|_| (StatusCode::BAD_REQUEST, "invalid cursor DID".to_string()))?;
                    let did_key = keys::repo_key(&did);
                    std::ops::Bound::Excluded(did_key)
                } else {
                    std::ops::Bound::Unbounded
                };

                let mut items = Vec::new();
                for item in db
                    .repos
                    .range((start_bound, std::ops::Bound::Unbounded))
                    .take(limit)
                {
                    let (k, v) = item
                        .into_inner()
                        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
                    let repo_state = crate::db::deser_repo_state(&v)
                        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
                    let did = crate::db::types::TrimmedDid::try_from(k.as_ref())
                        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?
                        .to_did();

                    items.push(RepoResponse {
                        did: did.to_string(),
                        status: repo_state.status.to_string(),
                        tracked: repo_state.tracked,
                        rev: repo_state.rev.as_ref().map(|r| r.to_string()),
                        last_updated_at: repo_state.last_updated_at,
                    });
                }
                Ok::<_, (StatusCode, String)>(items)
            }
            "resync" => {
                let start_bound = if let Some(cursor) = params.cursor {
                    let did = jacquard::types::did::Did::new_owned(&cursor)
                        .map_err(|_| (StatusCode::BAD_REQUEST, "invalid cursor DID".to_string()))?;
                    let did_key = keys::repo_key(&did);
                    std::ops::Bound::Excluded(did_key)
                } else {
                    std::ops::Bound::Unbounded
                };

                let mut items = Vec::new();
                for item in db
                    .resync
                    .range((start_bound, std::ops::Bound::Unbounded))
                    .take(limit)
                {
                    let (k, _) = item
                        .into_inner()
                        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

                    if let Ok(Some(v)) = db.repos.get(&k) {
                        let repo_state = crate::db::deser_repo_state(&v)
                            .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
                        let did = crate::db::types::TrimmedDid::try_from(k.as_ref())
                            .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?
                            .to_did();

                        items.push(RepoResponse {
                            did: did.to_string(),
                            status: repo_state.status.to_string(),
                            tracked: repo_state.tracked,
                            rev: repo_state.rev.as_ref().map(|r| r.to_string()),
                            last_updated_at: repo_state.last_updated_at,
                        });
                    }
                }
                Ok(items)
            }
            "pending" => {
                let start_bound = if let Some(cursor) = params.cursor {
                    let id = cursor
                        .parse::<u64>()
                        .map_err(|_| (StatusCode::BAD_REQUEST, "invalid cursor id".to_string()))?;
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
                    let (_, did_key) = item
                        .into_inner()
                        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

                    if let Ok(Some(v)) = db.repos.get(&did_key) {
                        let repo_state = crate::db::deser_repo_state(&v)
                            .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
                        let did = crate::db::types::TrimmedDid::try_from(did_key.as_ref())
                            .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?
                            .to_did();

                        items.push(RepoResponse {
                            did: did.to_string(),
                            status: repo_state.status.to_string(),
                            tracked: repo_state.tracked,
                            rev: repo_state.rev.as_ref().map(|r| r.to_string()),
                            last_updated_at: repo_state.last_updated_at,
                        });
                    }
                }
                Ok(items)
            }
            _ => Err((StatusCode::BAD_REQUEST, "invalid partition".to_string())),
        }?;

        Ok::<_, (StatusCode, String)>(results)
    })
    .await
    .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))??;

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

        for item in items {
            let did = match jacquard::types::did::Did::new_owned(&item.did) {
                Ok(d) => d,
                Err(_) => continue,
            };
            let did_key = keys::repo_key(&did);

            let existing_state = if let Ok(Some(bytes)) = db.repos.get(&did_key) {
                crate::db::deser_repo_state(&bytes)
                    .ok()
                    .map(|s| s.into_static())
            } else {
                None
            };

            if let Some(mut repo_state) = existing_state {
                if !repo_state.tracked {
                    let resync_bytes_opt = db.resync.get(&did_key).ok().flatten();
                    let old_gauge =
                        crate::db::Db::repo_gauge_state(&repo_state, resync_bytes_opt.as_deref());

                    repo_state.tracked = true;
                    // re-enqueue into pending
                    if let Ok(bytes) = ser_repo_state(&repo_state) {
                        batch.insert(&db.repos, &did_key, bytes);
                    }
                    batch.insert(
                        &db.pending,
                        keys::pending_key(repo_state.index_id),
                        &did_key,
                    );
                    batch.remove(&db.resync, &did_key);
                    gauge_transitions.push((old_gauge, GaugeState::Pending));
                }
            } else {
                let repo_state = RepoState::backfilling(rand::rng().next_u64());
                if let Ok(bytes) = ser_repo_state(&repo_state) {
                    batch.insert(&db.repos, &did_key, bytes);
                }
                batch.insert(
                    &db.pending,
                    keys::pending_key(repo_state.index_id),
                    &did_key,
                );
                added += 1;
                gauge_transitions.push((GaugeState::Synced, GaugeState::Pending)); // pseudo-transition to just inc pending
            }
        }

        batch
            .commit()
            .into_diagnostic()
            .map_err(|e| e.to_string())?;
        Ok::<_, String>((added, gauge_transitions))
    })
    .await
    .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?
    .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e))?;

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
            let did = match jacquard::types::did::Did::new_owned(&item.did) {
                Ok(d) => d,
                Err(_) => continue,
            };

            let delete_data = item.delete_data.unwrap_or(params.delete_data);
            let did_key = keys::repo_key(&did);

            let existing_state = if let Ok(Some(bytes)) = db.repos.get(&did_key) {
                crate::db::deser_repo_state(&bytes)
                    .ok()
                    .map(|s| s.into_static())
            } else {
                None
            };

            if let Some(mut repo_state) = existing_state {
                let resync_bytes_opt = db.resync.get(&did_key).ok().flatten();
                let old_gauge =
                    crate::db::Db::repo_gauge_state(&repo_state, resync_bytes_opt.as_deref());

                if delete_data {
                    if crate::ops::delete_repo(&mut batch, db, &did, &repo_state).is_ok() {
                        deleted_count += 1;
                        if old_gauge != GaugeState::Synced {
                            gauge_decrements.push(old_gauge);
                        }
                    } else {
                        tracing::error!("failed to apply delete_repo_batch to {}", did);
                    }
                } else if repo_state.tracked {
                    repo_state.tracked = false;
                    if let Ok(bytes) = ser_repo_state(&repo_state) {
                        batch.insert(&db.repos, &did_key, bytes);
                    }
                    batch.remove(&db.pending, keys::pending_key(repo_state.index_id));
                    batch.remove(&db.resync, &did_key);
                    if old_gauge != GaugeState::Synced {
                        gauge_decrements.push(old_gauge);
                    }
                }
            }
        }

        batch
            .commit()
            .into_diagnostic()
            .map_err(|e| e.to_string())?;

        Ok::<_, String>((deleted_count, gauge_decrements))
    })
    .await
    .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?
    .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e))?;

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
    let body_bytes = axum::body::to_bytes(req.into_body(), usize::MAX)
        .await
        .map_err(|e| (StatusCode::BAD_REQUEST, e.to_string()))?;

    let text =
        std::str::from_utf8(&body_bytes).map_err(|e| (StatusCode::BAD_REQUEST, e.to_string()))?;

    let trimmed = text.trim();
    if trimmed.starts_with('[') {
        serde_json::from_str::<Vec<RepoRequest>>(trimmed).map_err(|e| {
            (
                StatusCode::BAD_REQUEST,
                format!("invalid JSON array: {}", e),
            )
        })
    } else {
        trimmed
            .lines()
            .filter(|l| !l.trim().is_empty())
            .map(|line| {
                serde_json::from_str::<RepoRequest>(line).map_err(|e| {
                    (
                        StatusCode::BAD_REQUEST,
                        format!("invalid NDJSON line: {}", e),
                    )
                })
            })
            .collect()
    }
}
