pub mod links;
pub mod store;

pub(crate) mod api;

use std::ops::Bound;
use std::sync::Arc;

use miette::{IntoDiagnostic, Result};
use smol_str::SmolStr;
use tracing::warn;

use crate::state::AppState;

/// handle for querying the backlinks index.
#[derive(Clone)]
pub struct BacklinksControl(pub(crate) Arc<AppState>);

pub struct BacklinkEntry {
    pub uri: SmolStr,
    pub cid: SmolStr,
}

pub struct BacklinksPage {
    pub backlinks: Vec<BacklinkEntry>,
    /// raw key bytes of the last returned entry, used as cursor for next page
    pub next_cursor: Option<Vec<u8>>,
}

impl BacklinksControl {
    /// begin building a paginated fetch of backlinks pointing to `subject`.
    pub fn fetch(&self, subject: impl Into<String>) -> BacklinksFetch {
        BacklinksFetch {
            state: self.0.clone(),
            subject: subject.into(),
            collection: None,
            path: None,
            limit: 50,
            reverse: false,
            cursor: None,
        }
    }

    /// begin building a backlinks count query for `subject`.
    pub fn count(&self, subject: impl Into<String>) -> BacklinksCount {
        BacklinksCount {
            state: self.0.clone(),
            subject: subject.into(),
            collection: None,
            path: None,
        }
    }
}

/// a paginated fetch against the backlinks reverse index.
///
/// obtain via [`BacklinksControl::fetch`] and execute with [`BacklinksFetch::run`].
pub struct BacklinksFetch {
    state: Arc<AppState>,
    subject: String,
    collection: Option<String>,
    path: Option<String>,
    limit: usize,
    reverse: bool,
    cursor: Option<Vec<u8>>,
}

impl BacklinksFetch {
    /// filter results to the given source collection NSID.
    pub fn collection(mut self, collection: impl Into<String>) -> Self {
        self.collection = Some(collection.into());
        self
    }

    /// filter results to a specific dotted field path within the record (e.g. `.subject.uri`).
    /// has no effect unless a collection is also set.
    pub fn path(mut self, path: impl Into<String>) -> Self {
        self.path = Some(path.into());
        self
    }

    /// convenience: parse a `source` parameter of the form `collection` or `collection:path`
    /// and apply both filters. the path component (if present) has `.` prepended automatically.
    pub fn source(self, source: &str) -> Self {
        match source.split_once(':') {
            Some((col, p)) => self.collection(col).path(format!(".{p}")),
            None => self.collection(source),
        }
    }

    /// maximum number of results to return (default 50).
    pub fn limit(mut self, limit: usize) -> Self {
        self.limit = limit;
        self
    }

    /// if `true`, return results in reverse key order (default `false`).
    pub fn reverse(mut self, reverse: bool) -> Self {
        self.reverse = reverse;
        self
    }

    /// resume from the raw key bytes returned as `next_cursor` from a previous page.
    pub fn cursor(mut self, cursor: Vec<u8>) -> Self {
        self.cursor = Some(cursor);
        self
    }

    /// execute and return a page of backlink entries.
    pub async fn run(self) -> Result<BacklinksPage> {
        tokio::task::spawn_blocking(move || {
            let db = &self.state.db;
            let scan_prefix = store::reverse_scan_prefix(
                &self.subject,
                self.collection.as_deref(),
                self.path.as_deref(),
            );

            let iter: Box<dyn Iterator<Item = _>> = if !self.reverse {
                if let Some(ref cursor_bytes) = self.cursor {
                    Box::new(db.backlinks.range::<&[u8], _>((
                        Bound::Excluded(cursor_bytes.as_slice()),
                        Bound::Unbounded,
                    )))
                } else {
                    Box::new(db.backlinks.range(scan_prefix.as_slice()..))
                }
            } else {
                // for reverse scans, bound the end at the cursor (exclusive) or the
                // prefix end (increment last byte to get an exclusive upper bound)
                let end: Vec<u8> = if let Some(ref cursor_bytes) = self.cursor {
                    cursor_bytes.clone()
                } else {
                    let mut end = scan_prefix.clone();
                    if let Some(last) = end.last_mut() {
                        *last = last.saturating_add(1);
                    }
                    end
                };
                Box::new(
                    db.backlinks
                        .range(scan_prefix.as_slice()..end.as_slice())
                        .rev(),
                )
            };

            let mut backlinks = Vec::with_capacity(self.limit);
            let mut next_cursor = None;

            for item in iter {
                let key = item.key().into_diagnostic()?;
                if !key.starts_with(scan_prefix.as_slice()) {
                    break;
                }
                if backlinks.len() >= self.limit {
                    next_cursor = Some(key.to_vec());
                    break;
                }

                let Some((col, _path, did, rkey)) = store::source_from_reverse_key(&key) else {
                    warn!("backlinks: could not extract source from reverse key");
                    continue;
                };
                let Some(cid) = store::lookup_cid_from_ks(&db.records, did, col, rkey) else {
                    // record deleted after backlink was written
                    continue;
                };
                let at_uri = format!("at://{did}/{col}/{rkey}");
                backlinks.push(BacklinkEntry {
                    uri: SmolStr::new(at_uri),
                    cid: SmolStr::new(cid),
                });
            }

            Ok(BacklinksPage {
                backlinks,
                next_cursor,
            })
        })
        .await
        .into_diagnostic()?
    }
}

/// a count query against the backlinks reverse index.
///
/// obtain via [`BacklinksControl::count`] and execute with [`BacklinksCount::run`].
pub struct BacklinksCount {
    state: Arc<AppState>,
    subject: String,
    collection: Option<String>,
    path: Option<String>,
}

impl BacklinksCount {
    /// filter to the given source collection NSID.
    pub fn collection(mut self, collection: impl Into<String>) -> Self {
        self.collection = Some(collection.into());
        self
    }

    /// filter to a specific dotted field path within the record (e.g. `.subject.uri`).
    /// has no effect unless a collection is also set.
    pub fn path(mut self, path: impl Into<String>) -> Self {
        self.path = Some(path.into());
        self
    }

    /// parse a `source` parameter of the form `collection` or `collection:path`
    /// and apply both filters. the path component (if present) has `.` prepended automatically.
    pub fn source(self, source: &str) -> Self {
        match source.split_once(':') {
            Some((col, p)) => self.collection(col).path(format!(".{p}")),
            None => self.collection(source),
        }
    }

    /// execute and return the total count of matching entries.
    pub async fn run(self) -> Result<u64> {
        tokio::task::spawn_blocking(move || {
            let db = &self.state.db;
            let scan_prefix = store::reverse_scan_prefix(
                &self.subject,
                self.collection.as_deref(),
                self.path.as_deref(),
            );
            Ok(db.backlinks.prefix(&scan_prefix).count() as u64)
        })
        .await
        .into_diagnostic()?
    }
}
