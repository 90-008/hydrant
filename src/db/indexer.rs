use crate::types::{GaugeState, RepoStatus, ResyncState};
use fjall::{Keyspace, OwnedWriteBatch};
use jacquard_common::IntoStatic;
use jacquard_common::types::string::Did;
use miette::{IntoDiagnostic, Result, WrapErr};
use url::Url;

use crate::db::{Db, deser_repo_state, keys, ser_repo_state};
use crate::types::RepoState;

impl Db {
    pub(crate) fn update_gauge_diff(&self, old: &GaugeState, new: &GaugeState) {
        update_gauge_diff_impl!(self, old, new, update_count);
    }

    pub(crate) async fn update_gauge_diff_async(&self, old: &GaugeState, new: &GaugeState) {
        update_gauge_diff_impl!(self, old, new, update_count_async, await);
    }

    pub(crate) fn update_repo_state<F, T>(
        batch: &mut OwnedWriteBatch,
        repos: &Keyspace,
        did: &Did<'_>,
        f: F,
    ) -> Result<Option<(RepoState<'static>, T)>>
    where
        F: FnOnce(&mut RepoState, (&[u8], &mut fjall::OwnedWriteBatch)) -> Result<(bool, T)>,
    {
        let key = keys::repo_key(did);
        if let Some(bytes) = repos.get(&key).into_diagnostic()? {
            let mut state: RepoState = deser_repo_state(bytes.as_ref())?.into_static();
            let (changed, result) = f(&mut state, (key.as_slice(), batch))?;
            if changed {
                batch.insert(repos, key, ser_repo_state(&state)?);
            }
            Ok(Some((state, result)))
        } else {
            Ok(None)
        }
    }

    pub(crate) async fn update_repo_state_async<F, T>(
        &self,
        did: &Did<'_>,
        f: F,
    ) -> Result<Option<(RepoState<'static>, T)>>
    where
        F: FnOnce(&mut RepoState, (&[u8], &mut fjall::OwnedWriteBatch)) -> Result<(bool, T)>
            + Send
            + 'static,
        T: Send + 'static,
    {
        let mut batch = self.inner.batch();
        let repos = self.repos.clone();
        let did = did.clone().into_static();

        tokio::task::spawn_blocking(move || {
            let Some((state, t)) = Self::update_repo_state(&mut batch, &repos, &did, f)? else {
                return Ok(None);
            };
            batch.commit().into_diagnostic()?;
            Ok(Some((state, t)))
        })
        .await
        .into_diagnostic()?
    }

    pub(crate) fn repo_gauge_state(
        repo_state: &RepoState,
        resync_bytes: Option<&[u8]>,
    ) -> GaugeState {
        match repo_state.status {
            RepoStatus::Synced => GaugeState::Synced,
            RepoStatus::Error(_)
            | RepoStatus::Deactivated
            | RepoStatus::Takendown
            | RepoStatus::Suspended
            | RepoStatus::Deleted
            | RepoStatus::Desynchronized
            | RepoStatus::Throttled => resync_bytes
                .and_then(|b| rmp_serde::from_slice::<ResyncState>(b).ok())
                .and_then(|s| match s {
                    ResyncState::Error { kind, .. } => Some(GaugeState::Resync(Some(kind))),
                    _ => None,
                })
                .unwrap_or(GaugeState::Resync(None)),
        }
    }
}

pub fn set_record_count(
    batch: &mut OwnedWriteBatch,
    db: &Db,
    did: &Did<'_>,
    collection: &str,
    count: u64,
) {
    let key = keys::count_collection_key(did, collection);
    batch.insert(&db.counts, key, count.to_be_bytes());
}

pub fn update_record_count(
    batch: &mut OwnedWriteBatch,
    db: &Db,
    did: &Did<'_>,
    collection: &str,
    delta: i64,
) -> Result<()> {
    let key = keys::count_collection_key(did, collection);
    let count = db
        .counts
        .get(&key)
        .into_diagnostic()?
        .map(|v| -> Result<_> {
            Ok(u64::from_be_bytes(
                v.as_ref()
                    .try_into()
                    .into_diagnostic()
                    .wrap_err("expected to be count (8 bytes)")?,
            ))
        })
        .transpose()?
        .unwrap_or(0);
    let new_count = if delta >= 0 {
        count.saturating_add(delta as u64)
    } else {
        count.saturating_sub(delta.unsigned_abs())
    };
    batch.insert(&db.counts, key, new_count.to_be_bytes());
    Ok(())
}

pub fn get_record_count(db: &Db, did: &Did<'_>, collection: &str) -> Result<u64> {
    let key = keys::count_collection_key(did, collection);
    let count = db
        .counts
        .get(&key)
        .into_diagnostic()?
        .map(|v| -> Result<_> {
            Ok(u64::from_be_bytes(
                v.as_ref()
                    .try_into()
                    .into_diagnostic()
                    .wrap_err("expected to be count (8 bytes)")?,
            ))
        })
        .transpose()?;
    Ok(count.unwrap_or(0))
}

pub fn load_persisted_crawler_sources(
    db: &crate::db::Db,
) -> Result<Vec<crate::config::CrawlerSource>> {
    use crate::db::keys::CRAWLER_SOURCE_PREFIX;

    let mut sources = Vec::new();
    for entry in db.crawler.prefix(CRAWLER_SOURCE_PREFIX) {
        let (key, val) = entry.into_inner().into_diagnostic()?;
        let url_bytes = &key[CRAWLER_SOURCE_PREFIX.len()..];
        let url_str = std::str::from_utf8(url_bytes).into_diagnostic()?;
        let url = Url::parse(url_str).into_diagnostic()?;
        let mode: crate::config::CrawlerMode = rmp_serde::from_slice(&val).into_diagnostic()?;
        sources.push(crate::config::CrawlerSource { url, mode });
    }
    Ok(sources)
}
