use crate::api::xrpc::count_records::CountRecords;
use crate::api::xrpc::describe_repo::DescribeRepo;
use crate::control::Hydrant;
use axum::extract::FromRequest;
use axum::response::IntoResponse;
use axum::routing::get;
use axum::{Json, Router, extract::State, http::StatusCode};
use jacquard_api::com_atproto::repo::{
    get_record::{GetRecordError, GetRecordOutput, GetRecordRequest},
    list_records::{ListRecordsOutput, ListRecordsRequest, Record as RepoRecord},
};
use jacquard_api::com_atproto::sync::get_host_status::GetHostStatusRequest;
use jacquard_api::com_atproto::sync::get_repo_status::GetRepoStatusRequest;
use jacquard_api::com_atproto::sync::list_hosts::ListHostsRequest;
use jacquard_api::com_atproto::sync::list_repos::ListReposRequest;
use jacquard_common::types::ident::AtIdentifier;
use jacquard_common::xrpc::XrpcResp;
use jacquard_common::xrpc::{XrpcEndpoint, XrpcMethod};
use jacquard_common::{IntoStatic, xrpc::XrpcRequest};
use jacquard_common::{
    types::string::AtUri,
    xrpc::{GenericXrpcError, XrpcError},
};
use serde::{Deserialize, Serialize};
use smol_str::ToSmolStr;
use std::fmt::Display;

mod count_records;
mod describe_repo;
mod get_host_status;
mod get_record;
mod get_repo_status;
mod list_hosts;
mod list_records;
mod list_repos;

pub fn router() -> Router<Hydrant> {
    Router::new()
        .route(GetRecordRequest::PATH, get(get_record::handle))
        .route(ListRecordsRequest::PATH, get(list_records::handle))
        .route(CountRecords::PATH, get(count_records::handle))
        .route(DescribeRepo::PATH, get(describe_repo::handle))
        .route(GetHostStatusRequest::PATH, get(get_host_status::handle))
        .route(ListHostsRequest::PATH, get(list_hosts::handle))
        .route(GetRepoStatusRequest::PATH, get(get_repo_status::handle))
        .route(ListReposRequest::PATH, get(list_repos::handle))
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

fn upstream_error<E: std::error::Error + IntoStatic>(
    nsid: &'static str,
    message: impl Display,
) -> XrpcErrorResponse<E> {
    XrpcErrorResponse {
        status: StatusCode::BAD_GATEWAY,
        error: XrpcError::Generic(GenericXrpcError {
            error: "UpstreamError".into(),
            message: Some(message.to_smolstr()),
            nsid,
            method: "GET",
            http_status: StatusCode::BAD_GATEWAY,
        }),
    }
}
