/// hydrant configuration
pub mod config;
/// hydrant main api, includes the Hydrant type for programmatic control.
pub mod control;
pub(crate) mod filter;
pub(crate) mod pds_meta;
pub mod types;

/// dependencies hydrant uses in it's public api
pub mod deps {
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

pub(crate) mod api;
#[cfg(feature = "indexer")]
pub(crate) mod backfill;
#[cfg(feature = "backlinks")]
pub(crate) mod backlinks;
#[cfg(feature = "indexer")]
pub(crate) mod crawler;
pub(crate) mod db;
pub(crate) mod ingest;
#[cfg(feature = "indexer")]
pub(crate) mod ops;
pub(crate) mod patch;
pub(crate) mod resolver;
pub(crate) mod state;
pub(crate) mod util;

pub use filter::FilterMode;
