use tokio::sync::mpsc;

pub mod firehose;
#[cfg(feature = "indexer")]
pub mod indexer;
pub mod relay;
pub mod stream;
pub mod validation;
use url::Url;

use crate::ingest::stream::SubscribeReposMessage;

#[derive(Debug)]
pub enum IngestMessage {
    Firehose {
        url: Url,
        /// true when `relay` is a direct PDS connection (not an aggregating relay).
        /// enables host authority enforcement in the worker.
        is_pds: bool,
        msg: SubscribeReposMessage<'static>,
    },
}

pub type BufferTx = mpsc::Sender<IngestMessage>;
pub type BufferRx = mpsc::Receiver<IngestMessage>;
