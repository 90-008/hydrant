pub mod config;
pub mod control;
pub mod filter;
pub mod types;

#[cfg(all(feature = "relay", feature = "events", not(debug_assertions)))]
compile_error!("`relay` and `events` features are mutually exclusive");

#[cfg(all(feature = "relay", feature = "backlinks", not(debug_assertions)))]
compile_error!("`relay` and `backlinks` features are mutually exclusive");

pub(crate) mod api;
#[cfg(feature = "events")]
pub(crate) mod backfill;
#[cfg(feature = "backlinks")]
pub(crate) mod backlinks;
pub(crate) mod crawler;
pub(crate) mod db;
pub(crate) mod ingest;
#[cfg(feature = "events")]
pub(crate) mod ops;
pub(crate) mod resolver;
pub(crate) mod state;
pub(crate) mod util;
