pub mod config;
pub mod control;
pub mod filter;
pub mod types;

#[cfg(all(feature = "relay", feature = "indexer"))]
compile_error!("can't be relay and indexer at the same time");
#[cfg(all(feature = "relay", feature = "backlinks"))]
compile_error!("can't index backlinks while running as a relay");

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
