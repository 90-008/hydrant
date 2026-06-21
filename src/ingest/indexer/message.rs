use crate::ingest::mailbox::{ShardedMessage, ShardedReceiver, ShardedSender};
use crate::ingest::stream;
use jacquard_common::types::did::Did;
use miette::Result;
use url::Url;

#[derive(Debug)]
pub struct IndexerCommitData {
    pub commit: stream::Commit<'static>,
    /// true if the relay detected a gap (missing seq) before this commit
    /// and the indexer should trigger a backfill.
    pub chain_break: bool,
    /// result of parse_car_bytes, already done by relay so indexer does not re-parse.
    pub parsed_blocks: jacquard_repo::car::reader::ParsedCar,
}

#[derive(Debug)]
pub struct IndexerIdentityData {
    pub identity: stream::Identity<'static>,
    /// whether the identity actually changed (handle or key).
    pub changed: bool,
}

#[derive(Debug)]
pub struct IndexerAccountData {
    pub account: stream::Account<'static>,
    /// whether the repo was active prior to this event.
    pub was_active: bool,
    /// whether any state actually changed (active or status).
    pub changed: bool,
}

#[derive(Debug)]
#[allow(clippy::large_enum_variant)]
pub enum IndexerEventData {
    Commit(IndexerCommitData),
    Identity(IndexerIdentityData),
    Account(IndexerAccountData),
    Sync(Did<'static>),
}

#[derive(Debug)]
pub struct IndexerEvent {
    pub seq: i64,
    pub firehose: Url,
    pub data: IndexerEventData,
}

/// message sent from `relay_worker` to the indexer (`FirehoseWorker`) after
/// validation and repo-state management are done.
#[derive(Debug)]
pub enum IndexerMessage {
    /// a firehose event that passed relay-side validation.
    Event(Box<IndexerEvent>),
    /// a new repo was discovered and needs backfill.
    NewRepo(Did<'static>),
    /// backfill for this DID has completed; drain the resync buffer.
    BackfillFinished(Did<'static>),
}

#[derive(Clone, Debug)]
pub struct IndexerTx {
    pub(crate) inner: ShardedSender<IndexerMessage>,
}

pub type IndexerRx = ShardedReceiver<IndexerMessage>;

impl IndexerTx {
    pub async fn send(
        &self,
        msg: IndexerMessage,
    ) -> Result<(), tokio::sync::mpsc::error::SendError<IndexerMessage>> {
        self.inner.send(msg).await
    }

    pub fn blocking_send(
        &self,
        msg: IndexerMessage,
    ) -> Result<(), tokio::sync::mpsc::error::SendError<IndexerMessage>> {
        self.inner.blocking_send(msg)
    }
}

impl ShardedMessage for IndexerMessage {
    fn shard_idx(&self, num_shards: usize) -> usize {
        // keep commit, new-repo, and backfill-finished messages for a did on one ordered path.
        let did = match self {
            IndexerMessage::Event(e) => match &e.data {
                IndexerEventData::Commit(m) => &m.commit.repo,
                IndexerEventData::Identity(m) => &m.identity.did,
                IndexerEventData::Account(m) => &m.account.did,
                IndexerEventData::Sync(did) => did,
            },
            IndexerMessage::NewRepo(did) => did,
            IndexerMessage::BackfillFinished(did) => did,
        };

        (crate::util::hash(did) as usize) % num_shards
    }
}
