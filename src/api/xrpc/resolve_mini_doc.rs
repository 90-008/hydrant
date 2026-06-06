use jacquard_common::types::{did::Did, string::Handle};
use smol_str::SmolStr;

use crate::control::repos::MiniDocError;
use crate::db::types::DidKey;

use super::*;

#[derive(Serialize, Deserialize, jacquard_derive::IntoStatic)]
pub struct ResolveMiniDocOutput<'d> {
    #[serde(borrow)]
    pub did: Did<'d>,
    #[serde(borrow)]
    pub handle: Handle<'d>,
    #[serde(serialize_with = "crate::util::did_key_serialize_str")]
    #[serde(borrow)]
    pub signing_key: DidKey<'d>,
    pub pds: SmolStr,
}

pub struct BadExampleResolveMiniDocResponse;
impl XrpcResp for BadExampleResolveMiniDocResponse {
    const NSID: &'static str = "com.bad-example.identity.resolveMiniDoc";
    const ENCODING: &'static str = "application/json";
    type Output<'de> = ResolveMiniDocOutput<'de>;
    type Err<'de> = GenericXrpcError;
}

pub struct BlueMicrocosmResolveMiniDocResponse;
impl XrpcResp for BlueMicrocosmResolveMiniDocResponse {
    const NSID: &'static str = "blue.microcosm.identity.resolveMiniDoc";
    const ENCODING: &'static str = "application/json";
    type Output<'de> = ResolveMiniDocOutput<'de>;
    type Err<'de> = GenericXrpcError;
}

#[derive(Serialize, Deserialize, jacquard_derive::IntoStatic)]
pub struct ResolveMiniDocRequestData<'i> {
    #[serde(borrow)]
    pub identifier: AtIdentifier<'i>,
}

pub struct BadExampleResolveMiniDoc;
impl XrpcRequest for ResolveMiniDocRequestData<'_> {
    type Response = BadExampleResolveMiniDocResponse;
    const NSID: &'static str = Self::Response::NSID;
    const METHOD: XrpcMethod = XrpcMethod::Query;
}

impl XrpcEndpoint for BadExampleResolveMiniDoc {
    const PATH: &'static str = "/xrpc/com.bad-example.identity.resolveMiniDoc";
    const METHOD: XrpcMethod = XrpcMethod::Query;
    type Request<'de> = ResolveMiniDocRequestData<'de>;
    type Response = BadExampleResolveMiniDocResponse;
}

#[derive(Serialize, Deserialize, jacquard_derive::IntoStatic)]
pub struct BlueMicrocosmResolveMiniDocRequestData<'i> {
    #[serde(borrow)]
    pub identifier: AtIdentifier<'i>,
}

impl XrpcRequest for BlueMicrocosmResolveMiniDocRequestData<'_> {
    type Response = BlueMicrocosmResolveMiniDocResponse;
    const NSID: &'static str = Self::Response::NSID;
    const METHOD: XrpcMethod = XrpcMethod::Query;
}

pub struct BlueMicrocosmResolveMiniDoc;
impl XrpcEndpoint for BlueMicrocosmResolveMiniDoc {
    const PATH: &'static str = "/xrpc/blue.microcosm.identity.resolveMiniDoc";
    const METHOD: XrpcMethod = XrpcMethod::Query;
    type Request<'de> = BlueMicrocosmResolveMiniDocRequestData<'de>;
    type Response = BlueMicrocosmResolveMiniDocResponse;
}

pub(super) async fn resolve_mini_doc(
    hydrant: &Hydrant,
    identifier: &AtIdentifier<'_>,
    nsid: &'static str,
) -> XrpcResult<ResolveMiniDocOutput<'static>> {
    let did = hydrant
        .state
        .resolver
        .resolve_did(identifier)
        .await
        .map_err(|e| bad_request(nsid, format!("can't resolve identifier: {e}")))?;

    let doc = hydrant.repos.get(&did).mini_doc().await.map_err(|e| match e {
        MiniDocError::NotSynced => bad_request(nsid, "repo not synced"),
        MiniDocError::RepoNotFound => bad_request(nsid, "repo not found"),
        MiniDocError::CouldNotResolveIdentity => {
            upstream_error(nsid, "identity could not be resolved")
        }
        MiniDocError::Other(e) => internal_error(nsid, e),
    })?;

    Ok(ResolveMiniDocOutput {
        did: doc.did,
        handle: doc.handle,
        pds: doc.pds.to_smolstr(),
        signing_key: doc.signing_key,
    })
}

pub async fn handle_bad_example(
    State(hydrant): State<Hydrant>,
    ExtractXrpc(req): ExtractXrpc<BadExampleResolveMiniDoc>,
) -> XrpcResult<Json<ResolveMiniDocOutput<'static>>> {
    Ok(Json(
        resolve_mini_doc(
            &hydrant,
            &req.identifier,
            BadExampleResolveMiniDocResponse::NSID,
        )
        .await?,
    ))
}

pub async fn handle_blue_microcosm(
    State(hydrant): State<Hydrant>,
    ExtractXrpc(req): ExtractXrpc<BlueMicrocosmResolveMiniDoc>,
) -> XrpcResult<Json<ResolveMiniDocOutput<'static>>> {
    Ok(Json(
        resolve_mini_doc(
            &hydrant,
            &req.identifier,
            BlueMicrocosmResolveMiniDocResponse::NSID,
        )
        .await?,
    ))
}
