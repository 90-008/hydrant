#![allow(unused_imports)]

#[cfg(feature = "indexer")]
pub(crate) mod crawler;
pub(crate) mod filter;
pub(crate) mod firehose;
pub(crate) mod pds;
pub(crate) mod repos;
mod seed;
pub(crate) mod stream;

pub mod hydrant;
pub mod hosts;
pub mod db;
pub mod stats;

#[cfg(feature = "indexer")]
mod indexer;
#[cfg(feature = "indexer")]
pub use indexer::*;

#[cfg(feature = "relay")]
mod relay;
#[cfg(feature = "relay")]
pub use relay::*;
#[cfg(feature = "jetstream")]
mod jetstream;
#[cfg(feature = "jetstream")]
pub use jetstream::*;

pub use filter::{FilterControl, FilterPatch, FilterSnapshot};
#[cfg(feature = "firehose-diagnostics")]
pub use firehose::FirehoseDiagnosticsInfo;
pub use firehose::{FirehoseHandle, FirehoseSourceInfo};
pub use pds::{PdsControl, PdsTierAssignment, PdsTierDefinition};
pub use repos::{ListedRecord, Record, RecordList, RepoHandle, RepoInfo, ReposControl};

pub use hydrant::Hydrant;
pub use hosts::{ApiBinds, Host};
pub use db::DbControl;
pub use stats::StatsResponse;

#[cfg(feature = "indexer_stream")]
use crate::types::MarshallableEvt;

/// an event emitted by the hydrant event stream.
///
/// three variants are possible depending on the `type` field:
/// - `"record"`: a repo record was created, updated, or deleted. carries a [`RecordEvt`].
/// - `"identity"`: a DID's handle or PDS changed. carries an [`IdentityEvt`]. ephemeral, not replayable.
/// - `"account"`: a repo's active/inactive status changed. carries an [`AccountEvt`]. ephemeral, not replayable.
///
/// the `id` field is a monotonically increasing sequence number usable as a cursor for [`Hydrant::subscribe`].
#[cfg(feature = "indexer_stream")]
pub type Event = MarshallableEvt<'static>;

// Crate-internal re-exports for submodules using `super::*`
pub(crate) use std::sync::Arc;
pub(crate) use std::sync::atomic::{AtomicBool, Ordering};
pub(crate) use std::pin::Pin;
pub(crate) use std::task::{Context, Poll};
pub(crate) use futures::Stream;
pub(crate) use tokio::sync::mpsc;
pub(crate) use crate::state::AppState;
#[cfg(feature = "indexer_stream")]
use stream::event_stream_thread;
#[cfg(feature = "relay")]
use stream::relay_stream_thread;
#[cfg(feature = "jetstream")]
use stream::jetstream_stream_thread;
