use jacquard_api::com_atproto::sync::subscribe_repos::SubscribeReposMessage;
use tokio::sync::mpsc;

pub mod firehose;
pub mod worker;

pub type BufferedMessage = SubscribeReposMessage<'static>;

pub type BufferTx = mpsc::UnboundedSender<BufferedMessage>;
#[allow(dead_code)]
pub type BufferRx = mpsc::UnboundedReceiver<BufferedMessage>;
