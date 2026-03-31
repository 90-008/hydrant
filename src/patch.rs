use serde::{Deserialize, Serialize};

/// apply a bool patch or set replacement for a single set update.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub(crate) enum SetUpdate {
    /// replace the entire set with this list
    Set(Vec<String>),
    /// patch: true = add, false = remove
    Patch(std::collections::HashMap<String, bool>),
}
