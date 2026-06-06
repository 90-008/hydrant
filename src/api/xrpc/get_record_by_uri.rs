use jacquard_api::com_atproto::repo::get_record::{GetRecordError, GetRecordOutput};
use jacquard_common::{
    IntoStatic,
    cowstr::ToCowStr,
    types::{
        cid::Cid,
        ident::AtIdentifier,
        nsid::Nsid,
        recordkey::{RecordKey, Rkey},
        string::AtUri,
    },
    xrpc::{XrpcEndpoint, XrpcMethod, XrpcRequest, XrpcResp},
};

use super::*;

#[derive(Debug, Clone, PartialEq, Eq)]
struct RecordLookup {
    repo: AtIdentifier<'static>,
    collection: Nsid<'static>,
    rkey: RecordKey<Rkey<'static>>,
}

pub struct BlueMicrocosmGetRecordByUriResponse;
impl XrpcResp for BlueMicrocosmGetRecordByUriResponse {
    const NSID: &'static str = "blue.microcosm.repo.getRecordByUri";
    const ENCODING: &'static str = "application/json";
    type Output<'de> = GetRecordOutput<'de>;
    type Err<'de> = GetRecordError<'de>;
}

pub struct BadExampleGetUriRecordResponse;
impl XrpcResp for BadExampleGetUriRecordResponse {
    const NSID: &'static str = "com.bad-example.repo.getUriRecord";
    const ENCODING: &'static str = "application/json";
    type Output<'de> = GetRecordOutput<'de>;
    type Err<'de> = GetRecordError<'de>;
}

#[derive(Serialize, Deserialize, jacquard_derive::IntoStatic)]
pub struct GetRecordByUriRequestData<'i> {
    #[serde(borrow)]
    pub at_uri: AtUri<'i>,
    #[serde(default)]
    #[serde(borrow)]
    pub cid: Option<Cid<'i>>,
}

impl XrpcRequest for GetRecordByUriRequestData<'_> {
    type Response = BlueMicrocosmGetRecordByUriResponse;
    const NSID: &'static str = Self::Response::NSID;
    const METHOD: XrpcMethod = XrpcMethod::Query;
}

pub struct BlueMicrocosmGetRecordByUri;
impl XrpcEndpoint for BlueMicrocosmGetRecordByUri {
    const PATH: &'static str = "/xrpc/blue.microcosm.repo.getRecordByUri";
    const METHOD: XrpcMethod = XrpcMethod::Query;
    type Request<'de> = GetRecordByUriRequestData<'de>;
    type Response = BlueMicrocosmGetRecordByUriResponse;
}

#[derive(Serialize, Deserialize, jacquard_derive::IntoStatic)]
pub struct BadExampleGetUriRecordRequestData<'i> {
    #[serde(borrow)]
    pub at_uri: AtUri<'i>,
    #[serde(default)]
    #[serde(borrow)]
    pub cid: Option<Cid<'i>>,
}

impl XrpcRequest for BadExampleGetUriRecordRequestData<'_> {
    type Response = BadExampleGetUriRecordResponse;
    const NSID: &'static str = Self::Response::NSID;
    const METHOD: XrpcMethod = XrpcMethod::Query;
}

pub struct BadExampleGetUriRecord;
impl XrpcEndpoint for BadExampleGetUriRecord {
    const PATH: &'static str = "/xrpc/com.bad-example.repo.getUriRecord";
    const METHOD: XrpcMethod = XrpcMethod::Query;
    type Request<'de> = BadExampleGetUriRecordRequestData<'de>;
    type Response = BadExampleGetUriRecordResponse;
}

fn record_lookup_from_at_uri(at_uri: &AtUri<'_>) -> Result<RecordLookup, &'static str> {
    let Some(collection) = at_uri.collection() else {
        return Err("at-uri must include a collection");
    };
    let Some(rkey) = at_uri.rkey() else {
        return Err("at-uri must include an rkey");
    };

    Ok(RecordLookup {
        repo: at_uri.authority().clone().into_static(),
        collection: collection.clone().into_static(),
        rkey: rkey.clone().into_static(),
    })
}

fn matches_requested_cid(record_cid: &Cid<'_>, requested_cid: Option<&Cid<'_>>) -> bool {
    requested_cid.is_none_or(|cid| cid.as_str() == record_cid.as_str())
}

async fn get_record_by_uri(
    hydrant: Hydrant,
    at_uri: AtUri<'static>,
    cid: Option<Cid<'static>>,
    nsid: &'static str,
) -> Result<Json<GetRecordOutput<'static>>, XrpcErrorResponse<GetRecordError<'static>>> {
    let lookup = record_lookup_from_at_uri(&at_uri).map_err(|e| bad_request(nsid, e))?;
    let repo = hydrant
        .repos
        .resolve(&lookup.repo)
        .await
        .map_err(|e| internal_error(nsid, e))?;
    let record = repo
        .get_record(lookup.collection.as_str(), lookup.rkey.as_ref())
        .await
        .map_err(|e| internal_error(nsid, e))?;
    let Some(record) = record else {
        return Err(XrpcErrorResponse {
            status: StatusCode::NOT_FOUND,
            error: XrpcError::Xrpc(GetRecordError::RecordNotFound(None)),
        });
    };

    if !matches_requested_cid(&record.cid, cid.as_ref()) {
        return Err(XrpcErrorResponse {
            status: StatusCode::NOT_FOUND,
            error: XrpcError::Xrpc(GetRecordError::RecordNotFound(None)),
        });
    }

    Ok(Json(GetRecordOutput {
        uri: AtUri::from_parts_owned(
            record.did.as_str(),
            lookup.collection.as_str(),
            lookup.rkey.as_ref(),
        )
        .unwrap(),
        cid: Some(Cid::Str(record.cid.to_cowstr().into_static())),
        value: record.value,
        extra_data: Default::default(),
    }))
}

pub async fn handle_blue_microcosm(
    State(hydrant): State<Hydrant>,
    ExtractXrpc(req): ExtractXrpc<BlueMicrocosmGetRecordByUri>,
) -> Result<Json<GetRecordOutput<'static>>, XrpcErrorResponse<GetRecordError<'static>>> {
    get_record_by_uri(
        hydrant,
        req.at_uri,
        req.cid,
        BlueMicrocosmGetRecordByUriResponse::NSID,
    )
    .await
}

pub async fn handle_bad_example(
    State(hydrant): State<Hydrant>,
    ExtractXrpc(req): ExtractXrpc<BadExampleGetUriRecord>,
) -> Result<Json<GetRecordOutput<'static>>, XrpcErrorResponse<GetRecordError<'static>>> {
    get_record_by_uri(
        hydrant,
        req.at_uri,
        req.cid,
        BadExampleGetUriRecordResponse::NSID,
    )
    .await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn record_lookup_extracts_all_parts() {
        let at_uri = AtUri::new("at://bad-example.com/app.bsky.feed.post/3k4duaz5vfs2w").unwrap();
        let lookup = record_lookup_from_at_uri(&at_uri).unwrap();

        assert_eq!(lookup.repo.as_str(), "bad-example.com");
        assert_eq!(lookup.collection.as_str(), "app.bsky.feed.post");
        assert_eq!(lookup.rkey.as_ref(), "3k4duaz5vfs2w");
    }

    #[test]
    fn record_lookup_rejects_missing_collection() {
        let at_uri = AtUri::new("at://bad-example.com").unwrap();
        let err = record_lookup_from_at_uri(&at_uri).unwrap_err();

        assert_eq!(err, "at-uri must include a collection");
    }

    #[test]
    fn record_lookup_rejects_missing_rkey() {
        let at_uri = AtUri::new("at://bad-example.com/app.bsky.feed.post").unwrap();
        let err = record_lookup_from_at_uri(&at_uri).unwrap_err();

        assert_eq!(err, "at-uri must include an rkey");
    }

    #[test]
    fn matching_cid_is_accepted() {
        assert!(matches_requested_cid(
            &Cid::str("bafyrecord"),
            Some(&Cid::str("bafyrecord")),
        ));
    }

    #[test]
    fn mismatched_cid_is_rejected() {
        assert!(!matches_requested_cid(
            &Cid::str("bafyrecord"),
            Some(&Cid::str("bafyother")),
        ));
    }
}
