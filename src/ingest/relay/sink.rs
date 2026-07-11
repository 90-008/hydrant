//! per-mode event sinks for the shared firehose worker.
//!
//! the worker validates and persists repo state the same way in every mode;
//! what differs is where accepted events go: the indexer forwards them to the
//! `FirehoseWorker` hook, the relay re-sequences and re-emits them (plus
//! optional jetstream events), and a bare build drops them. each mode is a
//! parallel module with an identical interface, selected once here.

#[cfg(feature = "indexer")]
mod indexer;
#[cfg(feature = "indexer")]
pub(crate) use indexer::*;

#[cfg(feature = "relay")]
mod relay;
#[cfg(feature = "relay")]
pub(crate) use relay::*;

#[cfg(not(any(feature = "indexer", feature = "relay")))]
mod none;
#[cfg(not(any(feature = "indexer", feature = "relay")))]
pub(crate) use none::*;
