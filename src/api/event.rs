use serde::{Deserialize, Serialize};
use smol_str::SmolStr;

use crate::types::{IdentityEvt, RecordEvt};

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct MarshallableEvt {
    pub id: u64,
    #[serde(rename = "type")]
    pub event_type: SmolStr,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub record: Option<RecordEvt>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub identity: Option<IdentityEvt>,
}
