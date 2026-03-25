use std::collections::HashSet;

use futures::TryFutureExt;
use jacquard_common::types::{did::Did, nsid::Nsid, string::Handle};
use smol_str::SmolStr;

use crate::control::repos::MiniDocError;
use crate::db::types::DidKey;

use super::*;

#[derive(Serialize, Deserialize, jacquard_derive::IntoStatic)]
pub struct DescribeRepoOutput<'d> {
    #[serde(borrow)]
    pub did: Did<'d>,
    #[serde(borrow)]
    pub handle: Handle<'d>,
    #[serde(serialize_with = "crate::util::did_key_serialize_str")]
    #[serde(borrow)]
    pub signing_key: DidKey<'d>,
    pub pds: SmolStr,
    #[serde(borrow)]
    pub collections: HashSet<Nsid<'d>>,
}

pub struct DescribeRepoResponse;
impl jacquard_common::xrpc::XrpcResp for DescribeRepoResponse {
    const NSID: &'static str = "systems.gaze.hydrant.describeRepo";
    const ENCODING: &'static str = "application/json";
    type Output<'de> = DescribeRepoOutput<'de>;
    type Err<'de> = GenericXrpcError;
}

#[derive(Serialize, Deserialize, jacquard_derive::IntoStatic)]
pub struct DescribeRepoRequestData<'i> {
    #[serde(borrow)]
    pub identifier: AtIdentifier<'i>,
}

impl<'a> jacquard_common::xrpc::XrpcRequest for DescribeRepoRequestData<'a> {
    type Response = DescribeRepoResponse;
    const NSID: &'static str = Self::Response::NSID;
    const METHOD: jacquard_common::xrpc::XrpcMethod = jacquard_common::xrpc::XrpcMethod::Query;
}

pub struct DescribeRepo;
impl jacquard_common::xrpc::XrpcEndpoint for DescribeRepo {
    const PATH: &'static str = "/xrpc/systems.gaze.hydrant.describeRepo";
    const METHOD: jacquard_common::xrpc::XrpcMethod = jacquard_common::xrpc::XrpcMethod::Query;
    type Request<'de> = DescribeRepoRequestData<'de>;
    type Response = DescribeRepoResponse;
}

pub async fn handle(
    State(hydrant): State<Hydrant>,
    ExtractXrpc(req): ExtractXrpc<DescribeRepo>,
) -> XrpcResult<Json<DescribeRepoOutput<'static>>> {
    let nsid = DescribeRepoResponse::NSID;
    let did = hydrant
        .state
        .resolver
        .resolve_did(&req.identifier)
        .await
        .map_err(|e| internal_error(nsid, format!("can't resolve identifier: {e}")))?;

    let repo = hydrant.repos.get(&did);
    let doc = repo.mini_doc().map_err(|e| match e {
        MiniDocError::NotSynced => bad_request(nsid, "repo not synced"),
        MiniDocError::RepoNotFound => bad_request(nsid, "repo not found"),
        MiniDocError::CouldNotResolveIdentity => {
            upstream_error(nsid, "identity could not be resolved")
        }
        MiniDocError::Other(e) => internal_error(nsid, e),
    });
    let collections = repo.collections().map_err(|e| internal_error(nsid, e));
    let (doc, collections) = tokio::try_join!(doc, collections)?;

    Ok(Json(DescribeRepoOutput {
        did: doc.did,
        handle: doc.handle,
        pds: doc.pds.to_smolstr(),
        signing_key: doc.signing_key,
        collections: collections.into_iter().map(|(k, _)| k).collect(),
    }))
}
