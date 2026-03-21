use crate::control::Hydrant;
use axum::extract::FromRequest;
use axum::response::IntoResponse;
use axum::{Json, Router, extract::State, http::StatusCode};
use jacquard_api::com_atproto::repo::{
    get_record::{GetRecordError, GetRecordOutput, GetRecordRequest},
    list_records::{ListRecordsOutput, ListRecordsRequest, Record as RepoRecord},
};
use jacquard_common::types::ident::AtIdentifier;
use jacquard_common::xrpc::{XrpcEndpoint, XrpcMethod};
use jacquard_common::{IntoStatic, xrpc::XrpcRequest};
use jacquard_common::{
    types::string::AtUri,
    xrpc::{GenericXrpcError, XrpcError},
};
use serde::{Deserialize, Serialize};
use smol_str::ToSmolStr;
use std::fmt::Display;

pub fn router() -> Router<Hydrant> {
    Router::new()
        .route(
            GetRecordRequest::PATH,
            axum::routing::get(handle_get_record),
        )
        .route(
            ListRecordsRequest::PATH,
            axum::routing::get(handle_list_records),
        )
        .route(CountRecords::PATH, axum::routing::get(handle_count_records))
}

#[derive(Debug)]
pub struct XrpcErrorResponse<E: IntoStatic + std::error::Error = GenericXrpcError> {
    pub status: StatusCode,
    pub error: XrpcError<E>,
}

impl<E: Serialize + IntoStatic + std::error::Error> IntoResponse for XrpcErrorResponse<E> {
    fn into_response(self) -> axum::response::Response {
        (self.status, Json(self.error)).into_response()
    }
}

pub type XrpcResult<T, E = GenericXrpcError> = Result<T, XrpcErrorResponse<E>>;

pub struct ExtractXrpc<E: XrpcEndpoint>(pub E::Request<'static>);

impl<S, E> FromRequest<S> for ExtractXrpc<E>
where
    S: Send + Sync,
    E: XrpcEndpoint,
    E::Request<'static>: Send,
    for<'de> E::Request<'de>: Deserialize<'de> + IntoStatic<Output = E::Request<'static>>,
{
    type Rejection = XrpcErrorResponse<GenericXrpcError>;

    async fn from_request(
        req: axum::extract::Request,
        _state: &S,
    ) -> Result<Self, Self::Rejection> {
        let nsid = E::Request::<'static>::NSID;
        match E::METHOD {
            XrpcMethod::Query => {
                let query = req.uri().query().unwrap_or("");
                let res: E::Request<'_> =
                    serde_urlencoded::from_str(query).map_err(|e| bad_request(nsid, e))?;
                Ok(ExtractXrpc(res.into_static()))
            }
            XrpcMethod::Procedure(_) => {
                let body = axum::body::to_bytes(req.into_body(), usize::MAX)
                    .await
                    .map_err(|e| internal_error(nsid, e))?;
                let res: E::Request<'_> =
                    serde_json::from_slice(&body).map_err(|e| bad_request(nsid, e))?;
                Ok(ExtractXrpc(res.into_static()))
            }
        }
    }
}

fn internal_error<E: std::error::Error + IntoStatic>(
    nsid: &'static str,
    message: impl Display,
) -> XrpcErrorResponse<E> {
    XrpcErrorResponse {
        status: StatusCode::INTERNAL_SERVER_ERROR,
        error: XrpcError::Generic(GenericXrpcError {
            error: "InternalError".into(),
            message: Some(message.to_smolstr()),
            nsid,
            method: "GET",
            http_status: StatusCode::INTERNAL_SERVER_ERROR,
        }),
    }
}

fn bad_request<E: std::error::Error + IntoStatic>(
    nsid: &'static str,
    message: impl Display,
) -> XrpcErrorResponse<E> {
    XrpcErrorResponse {
        status: StatusCode::BAD_REQUEST,
        error: XrpcError::Generic(GenericXrpcError {
            error: "InvalidRequest".into(),
            message: Some(message.to_smolstr()),
            nsid,
            method: "GET",
            http_status: StatusCode::BAD_REQUEST,
        }),
    }
}

pub async fn handle_get_record(
    State(hydrant): State<Hydrant>,
    ExtractXrpc(req): ExtractXrpc<GetRecordRequest>,
) -> Result<Json<GetRecordOutput<'static>>, XrpcErrorResponse<GetRecordError<'static>>> {
    let record = hydrant
        .repos
        .resolve(&req.repo)
        .await
        .map_err(|e| internal_error(GetRecordRequest::PATH, e))?
        .get_record(&req.collection, &req.rkey.0)
        .await
        .map_err(|e| internal_error(GetRecordRequest::PATH, e))?;
    let Some(record) = record else {
        return Err(XrpcErrorResponse {
            status: StatusCode::NOT_FOUND,
            error: XrpcError::Xrpc(GetRecordError::RecordNotFound(None)),
        });
    };

    Ok(Json(GetRecordOutput {
        uri: AtUri::from_parts_owned(
            record.did.as_str(),
            req.collection.as_str(),
            req.rkey.0.as_str(),
        )
        .unwrap(),
        cid: Some(record.cid),
        value: record.value,
        extra_data: Default::default(),
    }))
}

pub async fn handle_list_records(
    State(hydrant): State<Hydrant>,
    ExtractXrpc(req): ExtractXrpc<ListRecordsRequest>,
) -> Result<Json<ListRecordsOutput<'static>>, XrpcErrorResponse<GenericXrpcError>> {
    let limit = req.limit.unwrap_or(50).min(100) as usize;
    let reverse = req.reverse.unwrap_or(false);
    let cursor = req.cursor.as_deref();

    let repo = hydrant
        .repos
        .resolve(&req.repo)
        .await
        .map_err(|e| internal_error(GetRecordRequest::PATH, e))?;
    let list = repo
        .list_records(req.collection.as_str(), limit, reverse, cursor)
        .await
        .map_err(|e| bad_request(ListRecordsRequest::PATH, e))?;

    let records = list
        .records
        .into_iter()
        .filter_map(|r| {
            let uri = AtUri::from_parts_owned(
                repo.did.as_str(),
                req.collection.as_str(),
                r.rkey.as_str(),
            )
            .ok()?;
            Some(RepoRecord {
                uri,
                cid: r.cid,
                value: r.value,
                extra_data: Default::default(),
            })
        })
        .collect();

    Ok(Json(ListRecordsOutput {
        records,
        cursor: list.cursor.map(|r| r.into()),
        extra_data: Default::default(),
    }))
}

#[derive(Serialize, Deserialize, jacquard_derive::IntoStatic)]
pub struct CountRecordsOutput {
    pub count: u64,
}

pub struct CountRecordsResponse;
impl jacquard_common::xrpc::XrpcResp for CountRecordsResponse {
    const NSID: &'static str = "systems.gaze.hydrant.countRecords";
    const ENCODING: &'static str = "application/json";
    type Output<'de> = CountRecordsOutput;
    type Err<'de> = GenericXrpcError;
}

#[derive(Serialize, Deserialize, jacquard_derive::IntoStatic)]
pub struct CountRecordsRequestData<'i> {
    #[serde(borrow)]
    pub identifier: AtIdentifier<'i>,
    pub collection: String,
}

impl<'a> jacquard_common::xrpc::XrpcRequest for CountRecordsRequestData<'a> {
    const NSID: &'static str = "systems.gaze.hydrant.countRecords";
    const METHOD: jacquard_common::xrpc::XrpcMethod = jacquard_common::xrpc::XrpcMethod::Query;
    type Response = CountRecordsResponse;
}

pub struct CountRecords;
impl jacquard_common::xrpc::XrpcEndpoint for CountRecords {
    const PATH: &'static str = "/xrpc/systems.gaze.hydrant.countRecords";
    const METHOD: jacquard_common::xrpc::XrpcMethod = jacquard_common::xrpc::XrpcMethod::Query;
    type Request<'de> = CountRecordsRequestData<'de>;
    type Response = CountRecordsResponse;
}

pub async fn handle_count_records(
    State(hydrant): State<Hydrant>,
    ExtractXrpc(req): ExtractXrpc<CountRecords>,
) -> XrpcResult<Json<CountRecordsOutput>> {
    let count = hydrant
        .repos
        .resolve(&req.identifier)
        .await
        .map_err(|e| internal_error(GetRecordRequest::PATH, e))?
        .count_records(&req.collection)
        .await
        .map_err(|e| internal_error(CountRecords::PATH, e))?;

    Ok(Json(CountRecordsOutput { count }))
}
