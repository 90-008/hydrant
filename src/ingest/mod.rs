pub mod firehose;
#[cfg(feature = "indexer")]
pub mod indexer;
mod mailbox;
pub mod relay;
pub mod stream;
pub mod validation;
use url::Url;

use crate::ingest::mailbox::{ShardedMessage, ShardedReceiver, ShardedSender};
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

#[derive(Clone, Debug)]
pub struct BufferTx {
    inner: ShardedSender<IngestMessage>,
}

pub type BufferRx = ShardedReceiver<IngestMessage>;

impl BufferTx {
    pub(crate) fn channel(num_shards: usize) -> (Self, Vec<BufferRx>) {
        let (inner, rxs) = ShardedSender::channel(num_shards);
        (Self { inner }, rxs)
    }

    pub async fn send(
        &self,
        msg: IngestMessage,
    ) -> Result<(), tokio::sync::mpsc::error::SendError<IngestMessage>> {
        self.inner.send(msg).await
    }
}

impl ShardedMessage for IngestMessage {
    fn shard_idx(&self, num_shards: usize) -> usize {
        let did = match self {
            IngestMessage::Firehose { msg, .. } => msg.did(),
        };

        did.map(|did| (crate::util::hash(did) as usize) % num_shards)
            .unwrap_or(0)
    }
}
