use tokio::sync::mpsc;

pub mod firehose;
pub mod stream;
pub mod validation;
pub mod worker;

use jacquard_common::types::did::Did;

use crate::ingest::stream::SubscribeReposMessage;
use url::Url;

#[derive(Debug)]
pub enum IngestMessage {
    Firehose {
        relay: Url,
        msg: SubscribeReposMessage<'static>,
    },
    BackfillFinished(Did<'static>),
}

pub type BufferTx = mpsc::UnboundedSender<IngestMessage>;
pub type BufferRx = mpsc::UnboundedReceiver<IngestMessage>;
