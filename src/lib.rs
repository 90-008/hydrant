/// hydrant configuration
pub mod config;
/// hydrant main api, includes the Hydrant type for programmatic control.
pub mod control;
pub(crate) mod filter;
#[cfg(feature = "relay")]
pub(crate) mod pds_daily_limit;
pub(crate) mod pds_meta;
pub mod types;

/// dependencies hydrant uses in it's public api
pub mod deps {
    #[cfg(feature = "user-keyspace")]
    pub use fjall;
    pub use futures;
    pub use jacquard_common as jacquard;
    pub use rustls;
    pub use smol_str;
}

#[cfg(all(
    feature = "relay",
    any(feature = "indexer", feature = "indexer_stream", feature = "backlinks")
))]
compile_error!("can't be relay and indexer at the same time");
#[cfg(all(
    not(feature = "indexer"),
    any(feature = "indexer_stream", feature = "backlinks")
))]
compile_error!("indexer dependent features (stream, backlinks) without indexer can't be enabled");
#[cfg(all(
    feature = "jetstream",
    not(any(feature = "indexer_stream", feature = "relay"))
))]
compile_error!("jetstream requires either indexer_stream or relay");

pub(crate) mod api;
#[cfg(feature = "indexer")]
pub(crate) mod backfill;
#[cfg(feature = "backlinks")]
pub mod backlinks;
pub(crate) mod car;
#[cfg(feature = "indexer")]
pub(crate) mod crawler;
pub(crate) mod db;
pub(crate) mod ingest;
#[cfg(feature = "jetstream")]
pub(crate) mod jetstream;
#[cfg(feature = "indexer")]
pub(crate) mod ops;
pub(crate) mod patch;
pub mod resolver;
#[cfg(feature = "indexer")]
pub(crate) mod sparse_mst;
pub(crate) mod state;
pub(crate) mod util;

pub use filter::FilterMode;
