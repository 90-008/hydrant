use jacquard_api::com_atproto::sync::request_crawl::{
    RequestCrawlError, RequestCrawlRequest, RequestCrawlResponse,
};
use miette::IntoDiagnostic;
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

    // enforce daily new pds limit on unknown hosts
    if !hydrant.firehose.is_source_known(&url) {
        let (allowed, to_persist) = hydrant.state.pds_daily_limit.try_increment();
        if !allowed {
            return Err(rate_limited(
                nsid,
                "daily limit for new PDS sources reached",
            ));
        }

        // persist the new count before returning so a crash cannot reset the counter
        // and allow the budget to be replayed.
        if let Some((day, count)) = to_persist {
            let state = hydrant.state.clone();
            tokio::task::spawn_blocking(move || {
                crate::db::save_pds_daily_adds(&state.db, day, count)
            })
            .await
            .into_diagnostic()
            .flatten()
            .map_err(|e| internal_error(nsid, e))?;
        }
    }

    hydrant
        .firehose
        .add_source(url, true)
        .await
        .map_err(|e| internal_error(nsid, e))?;

    Ok(StatusCode::OK)
}
