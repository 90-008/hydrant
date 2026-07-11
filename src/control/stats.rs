use super::Hydrant;
use miette::{IntoDiagnostic, Result};
use std::collections::BTreeMap;

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
        counts.insert("events", state.db.stream.approximate_event_count() as u64);

        #[cfg(feature = "relay")]
        counts.insert(
            "relay_events",
            state.db.relay.events.approximate_len() as u64,
        );
        #[cfg(feature = "jetstream")]
        counts.insert(
            "jetstream_events",
            state.db.jetstream.events.approximate_len() as u64,
        );

        let sizes = state.db.run(move |db| {
            Ok(db
                .all_keyspaces()
                .into_iter()
                .map(|(name, ks)| (name, ks.disk_space()))
                .collect::<BTreeMap<_, _>>())
        })
        .await?;

        Ok(StatsResponse { counts, sizes })
    }
}
