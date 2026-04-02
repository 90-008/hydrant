use jacquard_api::com_atproto::sync::request_crawl::{
    RequestCrawlError, RequestCrawlRequest, RequestCrawlResponse,
};
use url::Url;

use super::*;

pub async fn handle(
    State(hydrant): State<Hydrant>,
    ExtractXrpc(req): ExtractXrpc<RequestCrawlRequest>,
) -> XrpcResult<StatusCode, RequestCrawlError<'static>> {
    let nsid = RequestCrawlResponse::NSID;

    let url_str = format!("wss://{}/", req.hostname);
    let url = Url::parse(&url_str).map_err(|e| bad_request(nsid, e))?;

    if hydrant.pds.is_banned(&req.hostname) {
        return Err(XrpcErrorResponse {
            status: StatusCode::BAD_REQUEST,
            error: jacquard_common::xrpc::XrpcError::Xrpc(RequestCrawlError::HostBanned(Some(
                "host is banned".into(),
            ))),
        });
    }

    hydrant
        .firehose
        .add_source(url, true)
        .await
        .map_err(|e| internal_error(nsid, e))?;

    Ok(StatusCode::OK)
}
