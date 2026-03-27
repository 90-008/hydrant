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

    let (active, status) = repo_status_to_api(state.status);

    // rev is only meaningful when the repo is active and has been synced at least once
    let rev = active.then(|| state.rev.map(|r| r.to_tid())).flatten();

    Ok(Json(GetRepoStatusOutput {
        active,
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

pub(super) fn repo_status_to_api(status: RepoStatus) -> (bool, Option<ApiRepoStatus<'static>>) {
    match status {
        RepoStatus::Synced => (true, None),
        RepoStatus::Deactivated => (false, Some(ApiRepoStatus::Deactivated)),
        RepoStatus::Takendown => (false, Some(ApiRepoStatus::Takendown)),
        RepoStatus::Suspended => (false, Some(ApiRepoStatus::Suspended)),
        // we lost sync with this repo! report desynchronized
        // technicalllyyyy backfilling can mean the repo is active
        // because we are syncing it from the pds, but like also it is currently
        // desync'ed so...
        RepoStatus::Backfilling | RepoStatus::Error(_) => {
            (false, Some(ApiRepoStatus::Desynchronized))
        }
    }
}
