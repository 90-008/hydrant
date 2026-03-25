use super::*;

pub async fn handle(
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
