use std::str::FromStr;

use jacquard_api::com_atproto::sync::list_repos::{
    ListReposOutput, ListReposRequest, ListReposResponse, Repo,
};
use jacquard_common::CowStr;
use jacquard_common::types::cid::Cid;
use jacquard_common::types::string::Did;
use smol_str::ToSmolStr;

use crate::api::xrpc::get_repo_status::repo_status_to_api;

use super::*;

pub async fn handle(
    State(hydrant): State<Hydrant>,
    ExtractXrpc(req): ExtractXrpc<ListReposRequest>,
) -> XrpcResult<Json<ListReposOutput<'static>>> {
    let nsid = ListReposResponse::NSID;
    let limit = req.limit.unwrap_or(500).clamp(1, 1000) as usize;

    let cursor = req
        .cursor
        .as_deref()
        .map(Did::from_str)
        .transpose()
        .map_err(|e| bad_request(nsid, e))?;

    let (repos, next_cursor) = tokio::task::spawn_blocking(move || {
        let mut repos: Vec<Repo<'static>> = Vec::new();
        let mut next_cursor: Option<Did<'static>> = None;

        for item in hydrant.repos.iter_states(cursor.as_ref()) {
            let (did, state) = item?;

            // skip repos that haven't been synced at least once
            let Some(commit) = state.root else {
                continue;
            };

            let (active, status) = repo_status_to_api(state.status);
            repos.push(Repo {
                active: Some(active),
                did: did.clone(),
                head: Cid::from(commit.data),
                rev: commit.rev.to_tid(),
                status,
                extra_data: None,
            });

            if repos.len() >= limit {
                next_cursor = Some(did);
                break;
            }
        }

        Ok::<_, miette::Report>((repos, next_cursor))
    })
    .await
    .map_err(|e| internal_error(nsid, e))?
    .map_err(|e| internal_error(nsid, e))?;

    Ok(Json(ListReposOutput {
        cursor: next_cursor.map(|d| CowStr::Owned(d.as_str().to_smolstr())),
        repos,
        extra_data: None,
    }))
}
