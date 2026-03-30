use jacquard_api::com_atproto::sync::{
    get_repo_status::{
        GetRepoStatusError, GetRepoStatusOutput, GetRepoStatusOutputStatus, GetRepoStatusRequest,
        GetRepoStatusResponse,
    },
    list_repos::RepoStatus as ApiRepoStatus,
};

use crate::types::RepoStatus;

use super::*;

pub async fn handle(
    State(hydrant): State<Hydrant>,
    ExtractXrpc(req): ExtractXrpc<GetRepoStatusRequest>,
) -> XrpcResult<Json<GetRepoStatusOutput<'static>>, GetRepoStatusError<'static>> {
    let nsid = GetRepoStatusResponse::NSID;

    let Some(state) = hydrant
        .repos
        .get(&req.did)
        .state()
        .await
        .map_err(|e| internal_error(nsid, e))?
    else {
        return Err(XrpcErrorResponse {
            status: StatusCode::NOT_FOUND,
            error: XrpcError::Xrpc(GetRepoStatusError::RepoNotFound(None)),
        });
    };

    let status = repo_status_to_api(state.status);

    // rev is only meaningful when the repo is active and has been synced at least once
    let rev = state
        .active
        .then(|| state.root.map(|c| c.rev.to_tid()))
        .flatten();

    Ok(Json(GetRepoStatusOutput {
        active: state.active,
        did: req.did,
        rev,
        status: status.map(|s| match s {
            ApiRepoStatus::Takendown => GetRepoStatusOutputStatus::Takendown,
            ApiRepoStatus::Suspended => GetRepoStatusOutputStatus::Suspended,
            ApiRepoStatus::Deleted => GetRepoStatusOutputStatus::Deleted,
            ApiRepoStatus::Deactivated => GetRepoStatusOutputStatus::Deactivated,
            ApiRepoStatus::Desynchronized => GetRepoStatusOutputStatus::Desynchronized,
            ApiRepoStatus::Throttled => GetRepoStatusOutputStatus::Throttled,
            ApiRepoStatus::Other(v) => GetRepoStatusOutputStatus::Other(v),
        }),
        extra_data: None,
    }))
}

pub(super) fn repo_status_to_api(status: RepoStatus) -> Option<ApiRepoStatus<'static>> {
    match status {
        RepoStatus::Synced => None,
        RepoStatus::Deactivated => Some(ApiRepoStatus::Deactivated),
        RepoStatus::Takendown => Some(ApiRepoStatus::Takendown),
        RepoStatus::Suspended => Some(ApiRepoStatus::Suspended),
        RepoStatus::Deleted => Some(ApiRepoStatus::Deleted),
        // per spec, desynchronized and throttled have active=may-be-true
        RepoStatus::Desynchronized => Some(ApiRepoStatus::Desynchronized),
        RepoStatus::Throttled => Some(ApiRepoStatus::Throttled),
        RepoStatus::Error(_) => Some(ApiRepoStatus::Desynchronized),
    }
}
