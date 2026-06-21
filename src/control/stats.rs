use std::collections::BTreeMap;
use miette::{IntoDiagnostic, Result};
use super::Hydrant;

/// database statistics returned by [`Hydrant::stats`].
#[derive(serde::Serialize)]
pub struct StatsResponse {
    /// record counts per logical category (repos, records, events, error kinds, etc.)
    pub counts: BTreeMap<&'static str, u64>,
    /// on-disk size in bytes per keyspace
    pub sizes: BTreeMap<&'static str, u64>,
}

impl Hydrant {
    /// return database counts and on-disk sizes for all keyspaces.
    ///
    /// counts include: `repos`, `pending`, `resync`, `records`, `blocks`, `events`,
    /// `error_ratelimited`, `error_transport`, `error_generic`.
    ///
    /// sizes are in bytes, reported per keyspace.
    pub async fn stats(&self) -> Result<StatsResponse> {
        let state = self.state.clone();

        #[allow(unused_mut)]
        let mut count_keys = vec![
            "repos",
            "error_ratelimited",
            "error_transport",
            "error_generic",
        ];

        #[cfg(feature = "indexer")]
        {
            count_keys.push("pending");
            count_keys.push("records");
            count_keys.push("blocks");
            count_keys.push("resync");
        }

        #[cfg_attr(
            not(any(feature = "indexer_stream", feature = "relay")),
            allow(unused_mut)
        )]
        let mut counts: BTreeMap<&'static str, u64> =
            futures::future::join_all(count_keys.into_iter().map(|name| {
                let state = state.clone();
                async move { (name, state.db.get_count(name).await) }
            }))
            .await
            .into_iter()
            .collect();

        #[cfg(feature = "indexer_stream")]
        counts.insert("events", state.db.events.approximate_len() as u64);

        #[cfg(feature = "relay")]
        counts.insert(
            "relay_events",
            state.db.relay_events.approximate_len() as u64,
        );
        #[cfg(feature = "jetstream")]
        counts.insert(
            "jetstream_events",
            state.db.jetstream_events.approximate_len() as u64,
        );

        let sizes = tokio::task::spawn_blocking(move || {
            let mut s = BTreeMap::new();
            s.insert("repos", state.db.repos.disk_space());
            s.insert("cursors", state.db.cursors.disk_space());
            s.insert("counts", state.db.counts.disk_space());
            s.insert("filter", state.db.filter.disk_space());
            s.insert("crawler", state.db.crawler.disk_space());

            #[cfg(feature = "indexer")]
            {
                s.insert("records", state.db.records.disk_space());
                s.insert("blocks", state.db.blocks.disk_space());
                s.insert("pending", state.db.pending.disk_space());
                s.insert("resync", state.db.resync.disk_space());
                s.insert("resync_buffer", state.db.resync_buffer.disk_space());
            }
            #[cfg(feature = "indexer_stream")]
            s.insert("events", state.db.events.disk_space());

            #[cfg(feature = "relay")]
            s.insert("relay_events", state.db.relay_events.disk_space());
            #[cfg(feature = "jetstream")]
            s.insert("jetstream_events", state.db.jetstream_events.disk_space());

            #[cfg(feature = "backlinks")]
            s.insert("backlinks", state.db.backlinks.disk_space());

            s
        })
        .await
        .into_diagnostic()?;

        Ok(StatsResponse { counts, sizes })
    }
}
