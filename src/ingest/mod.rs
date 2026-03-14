use tokio::sync::mpsc;

pub mod firehose;
pub mod stream;
pub mod worker;

use jacquard_common::types::did::Did;

use crate::ingest::stream::SubscribeReposMessage;
use crate::util::RelayId;

#[derive(Debug)]
pub enum IngestMessage {
    Firehose {
        relay_id: RelayId,
        msg: SubscribeReposMessage<'static>,
    },
    BackfillFinished(Did<'static>),
}

pub type BufferTx = mpsc::UnboundedSender<IngestMessage>;
pub type BufferRx = mpsc::UnboundedReceiver<IngestMessage>;
