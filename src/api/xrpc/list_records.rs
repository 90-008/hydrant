use super::*;

pub async fn handle(
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
