use jacquard_api::com_atproto::sync::subscribe_repos::SubscribeReposMessage;
use tokio::sync::mpsc;

pub mod firehose;
pub mod worker;

use jacquard::types::did::Did;

#[derive(Debug)]
pub enum IngestMessage {
    Firehose(SubscribeReposMessage<'static>),
    BackfillFinished(Did<'static>),
}

pub type BufferedMessage = IngestMessage;

pub type BufferTx = mpsc::UnboundedSender<BufferedMessage>;
#[allow(dead_code)]
pub type BufferRx = mpsc::UnboundedReceiver<BufferedMessage>;
