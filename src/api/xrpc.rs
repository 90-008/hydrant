use crate::api::{AppState, XrpcResult};
use crate::db::types::DbRkey;
use crate::db::{self, Db, keys};
use axum::{Json, Router, extract::State, http::StatusCode};
use futures::TryFutureExt;
use jacquard::cowstr::ToCowStr;
use jacquard::types::ident::AtIdentifier;
use jacquard::{
    IntoStatic,
    api::com_atproto::repo::{
        get_record::{GetRecordError, GetRecordOutput, GetRecordRequest},
        list_records::{ListRecordsOutput, ListRecordsRequest, Record as RepoRecord},
    },
    xrpc::XrpcRequest,
};
use jacquard_api::com_atproto::repo::{get_record::GetRecord, list_records::ListRecords};
use jacquard_axum::{ExtractXrpc, IntoRouter, XrpcErrorResponse};
use jacquard_common::{
    types::{
        string::{AtUri, Cid},
        value::Data,
    },
    xrpc::{GenericXrpcError, XrpcError},
};
use miette::IntoDiagnostic;
use serde::{Deserialize, Serialize};
use smol_str::ToSmolStr;
use std::{fmt::Display, sync::Arc};
use tokio::task::spawn_blocking;

pub fn router() -> Router<Arc<AppState>> {
    Router::new()
        .merge(GetRecordRequest::into_router(handle_get_record))
        .merge(ListRecordsRequest::into_router(handle_list_records))
        .merge(CountRecords::into_router(handle_count_records))
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
    State(state): State<Arc<AppState>>,
    ExtractXrpc(req): ExtractXrpc<GetRecordRequest>,
) -> Result<Json<GetRecordOutput<'static>>, XrpcErrorResponse<GetRecordError<'static>>> {
    let db = &state.db;
    let did = state
        .resolver
        .resolve_did(&req.repo)
        .await
        .map_err(|e| bad_request(GetRecord::NSID, e))?;

    let partition = db
        .record_partition(req.collection.as_str())
        .map_err(|e| internal_error(GetRecord::NSID, e))?;

    let db_key = keys::record_key(&did, &DbRkey::new(req.rkey.0.as_str()));

    let cid_bytes = Db::get(partition, db_key)
        .await
        .map_err(|e| internal_error(GetRecord::NSID, e))?;

    if let Some(cid_bytes) = cid_bytes {
        // lookup block using binary cid
        let block_bytes = Db::get(db.blocks.clone(), &cid_bytes)
            .await
            .map_err(|e| internal_error(GetRecord::NSID, e))?
            .ok_or_else(|| internal_error(GetRecord::NSID, "not found"))?;

        let value: Data = serde_ipld_dagcbor::from_slice(&block_bytes)
            .map_err(|e| internal_error(GetRecord::NSID, e))?;

        let cid = Cid::new(&cid_bytes)
            .map_err(|e| internal_error(GetRecord::NSID, e))?
            .into_static();

        Ok(Json(GetRecordOutput {
            uri: AtUri::from_parts_owned(
                did.as_str(),
                req.collection.as_str(),
                req.rkey.0.as_str(),
            )
            .unwrap(),
            cid: Some(Cid::Str(cid.to_cowstr()).into_static()),
            value: value.into_static(),
            extra_data: Default::default(),
        }))
    } else {
        Err(XrpcErrorResponse {
            status: StatusCode::NOT_FOUND,
            error: XrpcError::Xrpc(GetRecordError::RecordNotFound(None)),
        })
    }
}

pub async fn handle_list_records(
    State(state): State<Arc<AppState>>,
    ExtractXrpc(req): ExtractXrpc<ListRecordsRequest>,
) -> Result<Json<ListRecordsOutput<'static>>, XrpcErrorResponse<GenericXrpcError>> {
    let db = &state.db;
    let did = state
        .resolver
        .resolve_did(&req.repo)
        .await
        .map_err(|e| bad_request(ListRecords::NSID, e))?;

    let ks = db
        .record_partition(req.collection.as_str())
        .map_err(|e| internal_error(ListRecords::NSID, e))?;

    let prefix = keys::record_prefix(&did);

    let limit = req.limit.unwrap_or(50).min(100) as usize;
    let reverse = req.reverse.unwrap_or(false);
    let blocks_ks = db.blocks.clone();

    let (results, cursor) = tokio::task::spawn_blocking(move || {
        let mut results = Vec::new();
        let mut cursor = None;

        let iter: Box<dyn Iterator<Item = _>> = if !reverse {
            let mut end_prefix = prefix.clone();
            if let Some(last) = end_prefix.last_mut() {
                *last += 1;
            }

            let end_key = if let Some(cursor) = &req.cursor {
                let mut k = prefix.clone();
                k.extend_from_slice(cursor.as_bytes());
                k
            } else {
                end_prefix
            };

            Box::new(ks.range(prefix.as_slice()..end_key.as_slice()).rev())
        } else {
            let start_key = if let Some(cursor) = &req.cursor {
                let mut k = prefix.clone();
                k.extend_from_slice(cursor.as_bytes());
                k.push(0);
                k
            } else {
                prefix.clone()
            };

            Box::new(ks.range(start_key.as_slice()..))
        };

        for item in iter {
            let (key, cid_bytes) = item.into_inner().into_diagnostic()?;

            if !key.starts_with(prefix.as_slice()) {
                break;
            }
            if results.len() >= limit {
                let key_str = String::from_utf8_lossy(&key);
                if let Some(last_part) = key_str.split(keys::SEP as char).last() {
                    cursor = Some(smol_str::SmolStr::from(last_part));
                }
                break;
            }

            let rkey: DbRkey = rmp_serde::from_slice(&key[prefix.len()..]).into_diagnostic()?;
            // look up using binary cid bytes from the record
            if let Ok(Some(block_bytes)) = blocks_ks.get(&cid_bytes) {
                let val: Data = serde_ipld_dagcbor::from_slice(&block_bytes).unwrap_or(Data::Null);
                let cid =
                    Cid::Str(Cid::new(&cid_bytes).into_diagnostic()?.to_cowstr()).into_static();
                results.push(RepoRecord {
                    uri: AtUri::from_parts_owned(
                        did.as_str(),
                        req.collection.as_str(),
                        rkey.to_smolstr(),
                    )
                    .into_diagnostic()?,
                    cid,
                    value: val.into_static(),
                    extra_data: Default::default(),
                });
            }
        }
        Result::<_, miette::Report>::Ok((results, cursor))
    })
    .await
    .map_err(|e| internal_error(ListRecords::NSID, e))?
    .map_err(|e| internal_error(ListRecords::NSID, e))?;

    Ok(Json(ListRecordsOutput {
        records: results,
        cursor: cursor.map(|c| c.into()),
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
pub struct CountRecordsRequest<'i> {
    #[serde(borrow)]
    pub identifier: AtIdentifier<'i>,
    pub collection: String,
}

impl<'a> jacquard_common::xrpc::XrpcRequest for CountRecordsRequest<'a> {
    const NSID: &'static str = "systems.gaze.hydrant.countRecords";
    const METHOD: jacquard_common::xrpc::XrpcMethod = jacquard_common::xrpc::XrpcMethod::Query;
    type Response = CountRecordsResponse;
}

pub struct CountRecords;
impl jacquard_common::xrpc::XrpcEndpoint for CountRecords {
    const PATH: &'static str = "/xrpc/systems.gaze.hydrant.countRecords";
    const METHOD: jacquard_common::xrpc::XrpcMethod = jacquard_common::xrpc::XrpcMethod::Query;
    type Request<'de> = CountRecordsRequest<'de>;
    type Response = CountRecordsResponse;
}

#[axum::debug_handler]
pub async fn handle_count_records(
    State(state): State<Arc<AppState>>,
    ExtractXrpc(req): ExtractXrpc<CountRecords>,
) -> XrpcResult<Json<CountRecordsOutput>> {
    let did = state
        .resolver
        .resolve_did(&req.identifier)
        .await
        .map_err(|e| bad_request(CountRecordsRequest::NSID, e))?;

    let count = spawn_blocking(move || {
        db::get_record_count(&state.db, &did, &req.collection)
            .map_err(|e| internal_error(CountRecordsRequest::NSID, e))
    })
    .map_err(|e| internal_error(CountRecordsRequest::NSID, e))
    .await??;

    Ok(Json(CountRecordsOutput { count }))
}
