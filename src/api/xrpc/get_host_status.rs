use jacquard_api::com_atproto::sync::get_host_status::{
    GetHostStatusError, GetHostStatusOutput, GetHostStatusRequest, GetHostStatusResponse,
};
use jacquard_common::CowStr;

use super::*;

pub async fn handle(
    State(hydrant): State<Hydrant>,
    ExtractXrpc(req): ExtractXrpc<GetHostStatusRequest>,
) -> XrpcResult<Json<GetHostStatusOutput<'static>>, GetHostStatusError<'static>> {
    let nsid = GetHostStatusResponse::NSID;

    let Some(host) = hydrant
        .get_host_status(&req.hostname)
        .await
        .map_err(|e| internal_error(nsid, e))?
    else {
        return Err(bad_request(nsid, "host does not exist"));
    };

    Ok(Json(GetHostStatusOutput {
        account_count: Some(host.account_count as i64),
        hostname: CowStr::Owned(host.name),
        seq: Some(host.seq),
        status: None,
        extra_data: None,
    }))
}
