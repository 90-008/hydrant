use std::sync::Arc;
use tracing::error;

use miette::{IntoDiagnostic, Result};

use crate::db::filter as db_filter;
use crate::filter::{FilterMode, SetUpdate};
use crate::state::AppState;

/// a point-in-time snapshot of the filter configuration. returned by all [`FilterControl`] methods.
///
/// because the filter is stored in the database and loaded on demand, this snapshot
/// may be stale if another caller modifies the filter concurrently. for the authoritative
/// live config use [`FilterControl::get`].
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct FilterSnapshot {
    pub mode: FilterMode,
    pub signals: Vec<String>,
    pub collections: Vec<String>,
    pub excludes: Vec<String>,
}

/// runtime control over the indexing filter.
///
/// the filter has two orthogonal axes:
///
/// **mode** controls discovery:
/// - [`FilterMode::Filter`]: only indexes repos whose firehose commits touch a collection
///   matching a configured `signal`. explicit [`ReposControl::track`] always works regardless.
/// - [`FilterMode::Full`]: indexes the entire network. `signals` are ignored for discovery
///   but `collections` and `excludes` still apply.
///
/// **sets** are each independently configurable:
/// - `signals`: NSID patterns that trigger auto-discovery in `filter` mode (e.g. `app.bsky.feed.post`, `app.bsky.graph.*`)
/// - `collections`: NSID patterns that filter which records are *stored*. empty means store all.
/// - `excludes`: DIDs that are always skipped regardless of mode.
///
/// NSID patterns support an optional `.*` suffix to match an entire namespace.
/// all mutations are persisted to the database and take effect immediately.
#[derive(Clone)]
pub struct FilterControl(pub(super) Arc<AppState>);

impl FilterControl {
    /// return the current filter configuration from the database.
    pub async fn get(&self) -> Result<FilterSnapshot> {
        let filter_ks = self.0.db.filter.clone();
        tokio::task::spawn_blocking(move || {
            let hot = db_filter::load(&filter_ks)?;
            let excludes = db_filter::read_set(&filter_ks, db_filter::EXCLUDE_PREFIX)?;
            Ok(FilterSnapshot {
                mode: hot.mode,
                signals: hot.signals.iter().map(|s| s.to_string()).collect(),
                collections: hot.collections.iter().map(|s| s.to_string()).collect(),
                excludes,
            })
        })
        .await
        .into_diagnostic()?
    }

    /// set the indexing mode. see [`FilterControl`] for mode semantics.
    pub fn set_mode(&self, mode: FilterMode) -> FilterPatch {
        FilterPatch::new(self).set_mode(mode)
    }

    /// replace the entire signals set. existing signals are removed.
    pub fn set_signals(&self, signals: impl IntoIterator<Item = impl Into<String>>) -> FilterPatch {
        FilterPatch::new(self).set_signals(signals)
    }

    /// add multiple signals without disturbing existing ones.
    pub fn append_signals(
        &self,
        signals: impl IntoIterator<Item = impl Into<String>>,
    ) -> FilterPatch {
        FilterPatch::new(self).append_signals(signals)
    }

    /// add a single signal. no-op if already present.
    pub fn add_signal(&self, signal: impl Into<String>) -> FilterPatch {
        FilterPatch::new(self).add_signal(signal)
    }

    /// remove a single signal. no-op if not present.
    pub fn remove_signal(&self, signal: impl Into<String>) -> FilterPatch {
        FilterPatch::new(self).remove_signal(signal)
    }

    /// replace the entire collections set. pass an empty iterator to store all collections.
    pub fn set_collections(
        &self,
        collections: impl IntoIterator<Item = impl Into<String>>,
    ) -> FilterPatch {
        FilterPatch::new(self).set_collections(collections)
    }

    /// add multiple collections without disturbing existing ones.
    pub fn append_collections(
        &self,
        collections: impl IntoIterator<Item = impl Into<String>>,
    ) -> FilterPatch {
        FilterPatch::new(self).append_collections(collections)
    }

    /// add a single collection filter. no-op if already present.
    pub fn add_collection(&self, collection: impl Into<String>) -> FilterPatch {
        FilterPatch::new(self).add_collection(collection)
    }

    /// remove a single collection filter. no-op if not present.
    pub fn remove_collection(&self, collection: impl Into<String>) -> FilterPatch {
        FilterPatch::new(self).remove_collection(collection)
    }

    /// replace the entire excludes set.
    pub fn set_excludes(
        &self,
        excludes: impl IntoIterator<Item = impl Into<String>>,
    ) -> FilterPatch {
        FilterPatch::new(self).set_excludes(excludes)
    }

    /// add multiple DIDs to the excludes set without disturbing existing ones.
    pub fn append_excludes(
        &self,
        excludes: impl IntoIterator<Item = impl Into<String>>,
    ) -> FilterPatch {
        FilterPatch::new(self).append_excludes(excludes)
    }

    /// add a single DID to the excludes set. no-op if already excluded.
    pub fn add_exclude(&self, did: impl Into<String>) -> FilterPatch {
        FilterPatch::new(self).add_exclude(did)
    }

    /// remove a single DID from the excludes set. no-op if not present.
    pub fn remove_exclude(&self, did: impl Into<String>) -> FilterPatch {
        FilterPatch::new(self).remove_exclude(did)
    }
}

/// a staged set of filter mutations. all methods accumulate changes without touching
/// the database. call [`FilterPatch::apply`] to commit the entire patch atomically.
///
/// obtain an instance by calling any mutation method on [`FilterControl`], or via
/// [`FilterPatch::new`] to start from a blank patch.
pub struct FilterPatch {
    state: Arc<AppState>,
    /// if set, replaces the current indexing mode.
    pub mode: Option<FilterMode>,
    /// if set, replaces or patches the signals set.
    pub(crate) signals: Option<SetUpdate>,
    /// if set, replaces or patches the collections set.
    pub(crate) collections: Option<SetUpdate>,
    /// if set, replaces or patches the excludes set.
    pub(crate) excludes: Option<SetUpdate>,
}

impl FilterPatch {
    /// create a new blank patch associated with the given [`FilterControl`].
    pub fn new(control: &FilterControl) -> Self {
        Self {
            state: control.0.clone(),
            mode: None,
            signals: None,
            collections: None,
            excludes: None,
        }
    }

    /// set the indexing mode. see [`FilterControl`] for mode semantics.
    pub fn set_mode(mut self, mode: FilterMode) -> Self {
        self.mode = Some(mode);
        self
    }

    /// replace the entire signals set. existing signals are removed.
    pub fn set_signals(mut self, signals: impl IntoIterator<Item = impl Into<String>>) -> Self {
        self.signals = Some(SetUpdate::Set(
            signals.into_iter().map(Into::into).collect(),
        ));
        self
    }

    /// add multiple signals without disturbing existing ones.
    pub fn append_signals(mut self, signals: impl IntoIterator<Item = impl Into<String>>) -> Self {
        self.signals = Some(SetUpdate::Patch(
            signals.into_iter().map(|s| (s.into(), true)).collect(),
        ));
        self
    }

    /// add a single signal. no-op if already present.
    pub fn add_signal(mut self, signal: impl Into<String>) -> Self {
        self.signals = Some(SetUpdate::Patch([(signal.into(), true)].into()));
        self
    }

    /// remove a single signal. no-op if not present.
    pub fn remove_signal(mut self, signal: impl Into<String>) -> Self {
        self.signals = Some(SetUpdate::Patch([(signal.into(), false)].into()));
        self
    }

    /// replace the entire collections set. pass an empty iterator to store all collections.
    pub fn set_collections(
        mut self,
        collections: impl IntoIterator<Item = impl Into<String>>,
    ) -> Self {
        self.collections = Some(SetUpdate::Set(
            collections.into_iter().map(Into::into).collect(),
        ));
        self
    }

    /// add multiple collections without disturbing existing ones.
    pub fn append_collections(
        mut self,
        collections: impl IntoIterator<Item = impl Into<String>>,
    ) -> Self {
        self.collections = Some(SetUpdate::Patch(
            collections.into_iter().map(|c| (c.into(), true)).collect(),
        ));
        self
    }

    /// add a single collection filter. no-op if already present.
    pub fn add_collection(mut self, collection: impl Into<String>) -> Self {
        self.collections = Some(SetUpdate::Patch([(collection.into(), true)].into()));
        self
    }

    /// remove a single collection filter. no-op if not present.
    pub fn remove_collection(mut self, collection: impl Into<String>) -> Self {
        self.collections = Some(SetUpdate::Patch([(collection.into(), false)].into()));
        self
    }

    /// replace the entire excludes set.
    pub fn set_excludes(mut self, excludes: impl IntoIterator<Item = impl Into<String>>) -> Self {
        self.excludes = Some(SetUpdate::Set(
            excludes.into_iter().map(Into::into).collect(),
        ));
        self
    }

    /// add multiple DIDs to the excludes set without disturbing existing ones.
    pub fn append_excludes(
        mut self,
        excludes: impl IntoIterator<Item = impl Into<String>>,
    ) -> Self {
        self.excludes = Some(SetUpdate::Patch(
            excludes.into_iter().map(|d| (d.into(), true)).collect(),
        ));
        self
    }

    /// add a single DID to the excludes set. no-op if already excluded.
    pub fn add_exclude(mut self, did: impl Into<String>) -> Self {
        self.excludes = Some(SetUpdate::Patch([(did.into(), true)].into()));
        self
    }

    /// remove a single DID from the excludes set. no-op if not present.
    pub fn remove_exclude(mut self, did: impl Into<String>) -> Self {
        self.excludes = Some(SetUpdate::Patch([(did.into(), false)].into()));
        self
    }

    /// commit the patch atomically to the database and update the in-memory filter.
    /// returns the updated [`FilterSnapshot`].
    pub async fn apply(self) -> Result<FilterSnapshot> {
        let filter_ks = self.state.db.filter.clone();
        let inner = self.state.db.inner.clone();
        let filter_handle = self.state.filter.clone();
        let mode = self.mode;
        let signals = self.signals;
        let collections = self.collections;
        let excludes = self.excludes;

        let new_filter = tokio::task::spawn_blocking(move || {
            let mut batch = inner.batch();
            db_filter::apply_patch(&mut batch, &filter_ks, mode, signals, collections, excludes)?;
            batch.commit().into_diagnostic()?;
            db_filter::load(&filter_ks)
        })
        .await
        .into_diagnostic()?
        .map_err(|e| {
            error!(err = %e, "failed to apply filter patch");
            e
        })?;

        let exclude_list = {
            let filter_ks = self.state.db.filter.clone();
            tokio::task::spawn_blocking(move || {
                db_filter::read_set(&filter_ks, db_filter::EXCLUDE_PREFIX)
            })
            .await
            .into_diagnostic()??
        };

        let snapshot = FilterSnapshot {
            mode: new_filter.mode,
            signals: new_filter.signals.iter().map(|s| s.to_string()).collect(),
            collections: new_filter
                .collections
                .iter()
                .map(|s| s.to_string())
                .collect(),
            excludes: exclude_list,
        };

        filter_handle.store(Arc::new(new_filter));
        Ok(snapshot)
    }
}
