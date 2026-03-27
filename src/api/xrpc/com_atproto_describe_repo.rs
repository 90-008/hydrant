use futures::TryFutureExt;
use jacquard_api::com_atproto::repo::describe_repo::{DescribeRepoOutput, DescribeRepoRequest};

use crate::util::invalid_handle;

use super::*;

pub async fn handle(
    State(hydrant): State<Hydrant>,
    ExtractXrpc(req): ExtractXrpc<DescribeRepoRequest>,
) -> XrpcResult<Json<DescribeRepoOutput<'static>>> {
    let nsid = "com.atproto.repo.describeRepo";
    let resolver = &hydrant.state.resolver;

    let did = resolver
        .resolve_did(&req.repo)
        .await
        .map_err(|e| bad_request(nsid, format!("could not resolve identifier: {e}")))?;

    let repo = hydrant.repos.get(&did);
    let ((did_doc, handle, handle_is_correct), collections) = tokio::try_join!(
        async {
            let (doc, handle) = resolver
                .resolve_raw_doc(&did)
                .map_err(|e| upstream_error(nsid, format!("could not resolve DID document: {e}")))
                .await?;
            let (handle, handle_is_correct) = if let Some(h) = handle {
                let is_correct = resolver
                    .verify_handle(&did, &h)
                    .map_err(|e| bad_request(nsid, format!("could not verify handle {h}: {e}")))
                    .await?;
                (h, is_correct)
            } else {
                (invalid_handle(), false)
            };
            Ok((doc, handle, handle_is_correct))
        },
        repo.collections().map_err(|e| internal_error(nsid, e)),
    )?;

    Ok(Json(DescribeRepoOutput {
        did,
        handle,
        handle_is_correct,
        did_doc,
        collections: collections.into_iter().map(|(k, _)| k).collect(),
        extra_data: Default::default(),
    }))
}
