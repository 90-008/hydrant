use super::Hydrant;
use crate::state::AppState;
use futures::FutureExt;
use miette::{IntoDiagnostic, Result};
use std::sync::Arc;

/// control over database maintenance operations.
///
/// all methods pause the crawler, firehose, and backfill worker for the duration
/// of the operation and restore their prior state on completion, whether or not
/// the operation succeeds.
#[derive(Clone)]
pub struct DbControl(pub(crate) Arc<AppState>);

impl DbControl {
    /// trigger a major compaction of all keyspaces in parallel.
    ///
    /// compaction reclaims disk space from deleted/updated keys and improves
    /// read performance. can take several minutes on large datasets.
    pub async fn compact(&self) -> Result<()> {
        let state = self.0.clone();
        state
            .with_ingestion_paused(async || state.db.compact().await)
            .await
    }

    /// train zstd compression dictionaries for every trainable keyspace
    /// (see `db::schema`: `repos`, `blocks`, `events`, and mode-dependent ones).
    ///
    /// dictionaries are written to `dict_{name}.bin` files inside the database folder.
    /// a restart is required to apply them. training samples data blocks from the
    /// existing database, so the database must have a reasonable amount of data first.
    pub async fn train_dicts(&self) -> Result<()> {
        let state = self.0.clone();
        state
            .with_ingestion_paused(async || {
                let train = |name: &'static str| {
                    let state = state.clone();
                    tokio::task::spawn_blocking(move || state.db.train_dict(name))
                        .map(|res: Result<_, _>| res.into_diagnostic().flatten())
                };
                futures::future::try_join_all(
                    crate::db::registry::trainable()
                        .into_iter()
                        .map(|(name, _)| train(name)),
                )
                .await
                .map(|_| ())
            })
            .await
    }

    /// open a user-defined keyspace.
    ///
    /// the keyspace name is prefixed to prevent collision with hydrant's internal keyspaces.
    #[cfg(feature = "user-keyspace")]
    pub fn open_keyspace<F>(&self, name: &str, setup: F) -> Result<fjall::Keyspace>
    where
        F: FnOnce() -> fjall::KeyspaceCreateOptions,
    {
        let name = format!("user:{name}");
        self.0.db.inner.keyspace(&name, setup).into_diagnostic()
    }

    /// create an owned write batch for batch operations.
    #[cfg(feature = "user-keyspace")]
    pub fn write_batch(&self) -> fjall::OwnedWriteBatch {
        self.0.db.inner.batch()
    }
}

#[cfg(all(test, feature = "user-keyspace"))]
mod tests {
    use super::*;
    use crate::config::Config;

    fn test_config(path: &std::path::Path) -> Config {
        Config {
            database_path: path.to_path_buf(),
            ..Default::default()
        }
    }

    #[tokio::test]
    async fn test_db_control_user_keyspace() -> Result<()> {
        let tmp = tempfile::tempdir().into_diagnostic()?;
        let cfg = test_config(tmp.path());
        let hydrant = Hydrant::new(cfg).await?;

        // 1. Open keyspace with custom options
        let ks = hydrant
            .db
            .open_keyspace("custom", fjall::KeyspaceCreateOptions::default)?;

        // 2. Put a key/value
        ks.insert("key1", "value1").into_diagnostic()?;
        assert_eq!(
            ks.get("key1").into_diagnostic()?.as_deref(),
            Some(b"value1" as &[u8])
        );

        // 3. Verify it is prefixed (maps to user_custom)
        let prefixed_ks = hydrant
            .state
            .db
            .inner
            .keyspace("user:custom", fjall::KeyspaceCreateOptions::default)
            .into_diagnostic()?;
        assert_eq!(
            prefixed_ks.get("key1").into_diagnostic()?.as_deref(),
            Some(b"value1" as &[u8])
        );

        // 4. Verify it does not map to unprefixed "custom"
        let unprefixed_ks = hydrant
            .state
            .db
            .inner
            .keyspace("custom", fjall::KeyspaceCreateOptions::default)
            .into_diagnostic()?;
        assert_eq!(
            unprefixed_ks.get("key1").into_diagnostic()?.as_deref(),
            None
        );

        // 5. Test batch functionality
        let mut batch = hydrant.db.write_batch();
        batch.insert(&ks, "key2", "value2");
        batch.commit().into_diagnostic()?;

        assert_eq!(
            ks.get("key2").into_diagnostic()?.as_deref(),
            Some(b"value2" as &[u8])
        );

        Ok(())
    }
}
