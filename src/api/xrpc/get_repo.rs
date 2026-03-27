use axum::{http::header, response::IntoResponse};
use jacquard_api::com_atproto::sync::get_repo::{GetRepoError, GetRepoRequest};
use jacquard_common::CowStr;

use crate::types::RepoStatus;

use super::*;

pub async fn handle(
    State(hydrant): State<Hydrant>,
    ExtractXrpc(req): ExtractXrpc<GetRepoRequest>,
) -> Result<impl IntoResponse, XrpcErrorResponse<GetRepoError<'static>>> {
    let nsid = GetRepoRequest::PATH;

    if req.since.is_some() {
        return Err(bad_request(
            nsid,
            "hydrant does not support since paramater",
        ));
    }

    let repo = hydrant.repos.get(&req.did);

    let Some(state) = repo.state().await.map_err(|e| internal_error(nsid, e))? else {
        return Err(XrpcErrorResponse {
            status: StatusCode::NOT_FOUND,
            error: XrpcError::Xrpc(GetRepoError::RepoNotFound(None)),
        });
    };

    let xrpc_err = match &state.status {
        RepoStatus::Takendown => Some(GetRepoError::RepoTakendown(None)),
        RepoStatus::Suspended => Some(GetRepoError::RepoSuspended(None)),
        RepoStatus::Deactivated => Some(GetRepoError::RepoDeactivated(None)),
        _ => None,
    };
    if let Some(err) = xrpc_err {
        return Err(XrpcErrorResponse {
            status: StatusCode::FORBIDDEN,
            error: XrpcError::Xrpc(err),
        });
    }

    let Some(car_bytes) = repo
        .generate_car()
        .await
        .map_err(|e| internal_error(nsid, e))?
    else {
        return Err(XrpcErrorResponse {
            status: StatusCode::NOT_FOUND,
            error: XrpcError::Xrpc(GetRepoError::RepoNotFound(Some(CowStr::Borrowed(
                "repo still backfilling",
            )))),
        });
    };

    Ok((
        [(header::CONTENT_TYPE, "application/vnd.ipld.car")],
        axum::body::Body::from_stream(car_bytes),
    ))
}
