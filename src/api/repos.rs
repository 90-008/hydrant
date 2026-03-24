use crate::control::{Hydrant, RepoInfo, repo_state_to_info};
use crate::db::keys;
use axum::{
    Json, Router,
    body::Body,
    extract::{Path, Query, State},
    http::{StatusCode, header},
    response::{IntoResponse, Response},
    routing::{delete, get, post, put},
};
use jacquard_common::types::did::Did;
use serde::Deserialize;

pub fn router() -> Router<Hydrant> {
    Router::new()
        .route("/repos", get(handle_get_repos))
        .route("/repos/resync", post(handle_post_resync))
        .route("/repos/{did}", get(handle_get_repo))
        .route("/repos", put(handle_put_repos))
        .route("/repos", delete(handle_delete_repos))
}

#[derive(Deserialize, Debug)]
pub struct RepoRequest {
    pub did: String,
}

#[derive(Deserialize)]
pub struct GetReposParams {
    pub limit: Option<usize>,
    pub cursor: Option<String>,
    pub partition: Option<String>,
}

pub async fn handle_get_repos(
    State(hydrant): State<Hydrant>,
    Query(params): Query<GetReposParams>,
) -> Result<Response, (StatusCode, String)> {
    let limit = params.limit.unwrap_or(100).min(1000);
    let partition = params.partition.unwrap_or_else(|| "all".to_string());

    let items = tokio::task::spawn_blocking(move || {
        let db = &hydrant.state.db;

        let to_info = |k: &[u8], v: &[u8]| -> Result<RepoInfo, (StatusCode, String)> {
            let repo_state = crate::db::deser_repo_state(v).map_err(internal)?;
            let did = crate::db::types::TrimmedDid::try_from(k)
                .map_err(internal)?
                .to_did();

            Ok(repo_state_to_info(did, repo_state))
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

                    items.push(to_info(&k, &repo_state_bytes)?);
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
                        items.push(to_info(&did_key, &v)?);
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

pub async fn handle_get_repo(
    State(hydrant): State<Hydrant>,
    Path(did_str): Path<String>,
) -> Result<Json<RepoInfo>, (StatusCode, String)> {
    let did = Did::new(&did_str).map_err(bad_request)?;

    let item = hydrant
        .repos
        .info(&did)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

    item.map(Json)
        .ok_or_else(|| (StatusCode::NOT_FOUND, "repository not found".to_string()))
}

pub async fn handle_put_repos(
    State(hydrant): State<Hydrant>,
    req: axum::extract::Request,
) -> Result<Json<Vec<String>>, (StatusCode, String)> {
    let items = parse_body(req).await?;

    let dids: Vec<Did<'static>> = items
        .into_iter()
        .filter_map(|item| Did::new_owned(&item.did).ok())
        .collect();

    let queued = hydrant
        .repos
        .track(dids)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

    Ok(Json(queued.into_iter().map(|d| d.to_string()).collect()))
}

pub async fn handle_delete_repos(
    State(hydrant): State<Hydrant>,
    req: axum::extract::Request,
) -> Result<Json<Vec<String>>, (StatusCode, String)> {
    let items = parse_body(req).await?;

    let dids: Vec<Did<'static>> = items
        .into_iter()
        .filter_map(|item| Did::new_owned(&item.did).ok())
        .collect();

    let untracked = hydrant
        .repos
        .untrack(dids)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

    Ok(Json(untracked.into_iter().map(|d| d.to_string()).collect()))
}

pub async fn handle_post_resync(
    State(hydrant): State<Hydrant>,
    req: axum::extract::Request,
) -> Result<Json<Vec<String>>, (StatusCode, String)> {
    let items = parse_body(req).await?;

    let dids: Vec<Did<'static>> = items
        .into_iter()
        .filter_map(|item| Did::new_owned(&item.did).ok())
        .collect();

    let queued = hydrant
        .repos
        .resync(dids)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

    Ok(Json(queued.into_iter().map(|d| d.to_string()).collect()))
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
