use super::*;

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

pub async fn handle(
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
