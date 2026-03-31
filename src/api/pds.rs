use std::collections::HashMap;

use axum::{
    Json, Router,
    extract::State,
    http::StatusCode,
    routing::{delete, get, put},
};
use serde::{Deserialize, Serialize};

use crate::control::{Hydrant, PdsTierAssignment, PdsTierDefinition};

pub fn router() -> Router<Hydrant> {
    Router::new()
        .route("/pds/tiers", get(list_tiers))
        .route("/pds/tiers", put(set_tier))
        .route("/pds/tiers", delete(remove_tier))
        .route("/pds/rate-tiers", get(list_rate_tiers))
}

/// combined response: tier assignments + available tier definitions.
#[derive(Serialize)]
pub struct TiersResponse {
    pub assignments: Vec<PdsTierAssignment>,
    pub rate_tiers: HashMap<String, PdsTierDefinition>,
}

pub async fn list_tiers(State(hydrant): State<Hydrant>) -> Json<TiersResponse> {
    Json(TiersResponse {
        assignments: hydrant.pds.list_assignments().await,
        rate_tiers: hydrant.pds.list_rate_tiers(),
    })
}

pub async fn list_rate_tiers(
    State(hydrant): State<Hydrant>,
) -> Json<HashMap<String, PdsTierDefinition>> {
    Json(hydrant.pds.list_rate_tiers())
}

#[derive(Deserialize)]
pub struct SetTierBody {
    pub host: String,
    pub tier: String,
}

pub async fn set_tier(
    State(hydrant): State<Hydrant>,
    Json(body): Json<SetTierBody>,
) -> Result<StatusCode, (StatusCode, String)> {
    hydrant
        .pds
        .set_tier(body.host, body.tier)
        .await
        .map(|_| StatusCode::OK)
        .map_err(|e| (StatusCode::BAD_REQUEST, e.to_string()))
}

#[derive(Deserialize)]
pub struct RemoveTierBody {
    pub host: String,
}

pub async fn remove_tier(
    State(hydrant): State<Hydrant>,
    Json(body): Json<RemoveTierBody>,
) -> Result<StatusCode, (StatusCode, String)> {
    hydrant
        .pds
        .remove_tier(body.host)
        .await
        .map(|_| StatusCode::OK)
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))
}
