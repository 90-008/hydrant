use std::sync::Arc;

use tokio::sync::mpsc;

const SHARD_CHANNEL_CAPACITY: usize = 64;

pub(crate) trait ShardedMessage {
    fn shard_idx(&self, num_shards: usize) -> usize;
}

#[derive(Debug)]
pub(crate) struct ShardedSender<T> {
    shards: Arc<[mpsc::Sender<T>]>,
}

pub(crate) type ShardedReceiver<T> = mpsc::Receiver<T>;

impl<T> Clone for ShardedSender<T> {
    fn clone(&self) -> Self {
        Self {
            shards: Arc::clone(&self.shards),
        }
    }
}

impl<T: ShardedMessage> ShardedSender<T> {
    pub(crate) fn channel(num_shards: usize) -> (Self, Vec<ShardedReceiver<T>>) {
        assert!(num_shards > 0, "num_shards must be greater than zero");

        let mut txs = Vec::with_capacity(num_shards);
        let mut rxs = Vec::with_capacity(num_shards);
        for _ in 0..num_shards {
            let (tx, rx) = mpsc::channel(SHARD_CHANNEL_CAPACITY);
            txs.push(tx);
            rxs.push(rx);
        }

        (
            Self {
                shards: Arc::from(txs.into_boxed_slice()),
            },
            rxs,
        )
    }

    pub(crate) async fn send(&self, msg: T) -> Result<(), mpsc::error::SendError<T>> {
        let shard_idx = msg.shard_idx(self.shards.len());
        self.shards[shard_idx].send(msg).await
    }

    #[cfg(feature = "indexer")]
    pub(crate) fn blocking_send(&self, msg: T) -> Result<(), mpsc::error::SendError<T>> {
        let shard_idx = msg.shard_idx(self.shards.len());
        self.shards[shard_idx].blocking_send(msg)
    }
}
