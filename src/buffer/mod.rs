use jacquard::{api::com_atproto::sync::subscribe_repos::SubscribeReposMessage, types::did::Did};

pub mod processor;

pub struct BufferedMessage {
    pub did: Did<'static>,
    pub msg: SubscribeReposMessage<'static>,
    pub buffered_at: i64,
}
