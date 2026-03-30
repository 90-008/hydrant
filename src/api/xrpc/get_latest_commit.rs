use jacquard_api::com_atproto::sync::get_latest_commit::{
    GetLatestCommitError, GetLatestCommitOutput, GetLatestCommitRequest, GetLatestCommitResponse,
};
use jacquard_common::{CowStr, cowstr::ToCowStr, types::cid::Cid};

use crate::types::RepoStatus;

use super::*;

pub async fn handle(
    State(hydrant): State<Hydrant>,
    ExtractXrpc(req): ExtractXrpc<GetLatestCommitRequest>,
) -> XrpcResult<Json<GetLatestCommitOutput<'static>>, GetLatestCommitError<'static>> {
    let nsid = GetLatestCommitResponse::NSID;

    let Some(state) = hydrant
        .repos
        .get(&req.did)
        .state()
        .await
        .map_err(|e| internal_error(nsid, e))?
    else {
        return Err(XrpcErrorResponse {
            status: StatusCode::NOT_FOUND,
            error: XrpcError::Xrpc(GetLatestCommitError::RepoNotFound(None)),
        });
    };

    // return specific errors for inactive account states
    let xrpc_err = match &state.status {
        RepoStatus::Takendown => Some(GetLatestCommitError::RepoTakendown(None)),
        RepoStatus::Suspended => Some(GetLatestCommitError::RepoSuspended(None)),
        RepoStatus::Deactivated => Some(GetLatestCommitError::RepoDeactivated(None)),
        RepoStatus::Deleted => Some(GetLatestCommitError::RepoNotFound(Some(CowStr::Borrowed(
            "deleted",
        )))),
        _ => None,
    };
    if let Some(err) = xrpc_err {
        return Err(XrpcErrorResponse {
            status: StatusCode::FORBIDDEN,
            error: XrpcError::Xrpc(err),
        });
    }

    // return whatever we last recorded; if we haven't synced at all yet, we have nothing to give
    let Some(commit) = state.root else {
        return Err(XrpcErrorResponse {
            status: StatusCode::NOT_FOUND,
            error: XrpcError::Xrpc(GetLatestCommitError::RepoNotFound(Some(CowStr::Borrowed(
                "repo still backfilling",
            )))),
        });
    };

    let Some(atp_commit) = commit.into_atp_commit(req.did) else {
        return Err(internal_error(nsid, "repo needs migration"));
    };

    let commit_cid = atp_commit.to_cid().map_err(|e| internal_error(nsid, e))?;

    Ok(Json(GetLatestCommitOutput {
        cid: Cid::Str(commit_cid.to_cowstr().into_static()),
        rev: atp_commit.rev,
        extra_data: None,
    }))
}
