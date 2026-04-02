use jacquard_api::com_atproto::sync::{
    HostStatus,
    list_hosts::{Host, ListHostsOutput, ListHostsRequest, ListHostsResponse},
};
use jacquard_common::CowStr;

use super::*;

pub async fn handle(
    State(hydrant): State<Hydrant>,
    ExtractXrpc(req): ExtractXrpc<ListHostsRequest>,
) -> XrpcResult<Json<ListHostsOutput<'static>>> {
    let nsid = ListHostsResponse::NSID;
    let limit = req.limit.unwrap_or(200).clamp(1, 1000) as usize;

    let (hosts, cursor) = hydrant
        .list_hosts(req.cursor.as_deref(), limit)
        .await
        .map_err(|e| internal_error(nsid, e))?;

    Ok(Json(ListHostsOutput {
        cursor: cursor.map(CowStr::Owned),
        hosts: hosts
            .into_iter()
            .map(|h| Host {
                hostname: CowStr::Owned(h.name),
                seq: Some(h.seq),
                status: h.is_banned.then_some(HostStatus::Banned),
                account_count: Some(h.account_count as i64),
                extra_data: None,
            })
            .collect(),
        extra_data: None,
    }))
}
