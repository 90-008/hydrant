use serde::{Deserialize, Serialize};
use super::FirehoseError;
use super::types::{SubscribeReposMessage, Info, InfoName};

#[derive(Debug, Deserialize, Serialize)]
struct EventHeader {
    op: i64,
    t: Option<String>,
}

#[derive(Deserialize)]
struct ErrorFrame {
    error: String,
    message: Option<String>,
}

pub fn decode_frame<'i>(bytes: &'i [u8]) -> Result<SubscribeReposMessage<'i>, FirehoseError> {
    let mut de = serde_ipld_dagcbor::de::Deserializer::from_slice(bytes);
    let header = EventHeader::deserialize(&mut de)?;

    match header.op {
        -1 => {
            let err = ErrorFrame::deserialize(&mut de)?;
            return Err(FirehoseError::RelayError {
                error: err.error,
                message: err.message,
            });
        }
        1 => {}
        op => return Err(FirehoseError::UnknownOp(op)),
    }

    let t = header.t.ok_or(FirehoseError::MissingType)?;

    let msg = match t.as_str() {
        "#commit" => SubscribeReposMessage::Commit(Box::new(Deserialize::deserialize(&mut de)?)),
        "#account" => SubscribeReposMessage::Account(Box::new(Deserialize::deserialize(&mut de)?)),
        "#identity" => {
            SubscribeReposMessage::Identity(Box::new(Deserialize::deserialize(&mut de)?))
        }
        "#sync" => SubscribeReposMessage::Sync(Box::new(Deserialize::deserialize(&mut de)?)),
        "#info" => {
            let info: Info<'i> = Deserialize::deserialize(&mut de)?;
            if info.name == InfoName::OutdatedCursor {
                return Err(FirehoseError::FutureCursor);
            }
            SubscribeReposMessage::Info(Box::new(info))
        }
        other => return Err(FirehoseError::UnknownType(other.to_string())),
    };

    Ok(msg)
}

#[cfg(feature = "relay")]
#[derive(Serialize)]
struct EncodeHeader<'a> {
    op: i64,
    t: &'a str,
}

#[cfg(feature = "relay")]
#[derive(Serialize)]
struct EncodeErrorHeader {
    op: i64,
}

#[cfg(feature = "relay")]
#[derive(Serialize)]
struct EncodeErrorFrame<'a> {
    error: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    message: Option<&'a str>,
}

#[cfg(feature = "relay")]
pub fn encode_frame<T: serde::Serialize>(t: &str, msg: &T) -> miette::Result<bytes::Bytes> {
    let mut buf = serde_ipld_dagcbor::to_vec(&EncodeHeader { op: 1, t })
        .map_err(|e| miette::miette!("encode_frame header: {e}"))?;
    buf.extend_from_slice(
        &serde_ipld_dagcbor::to_vec(msg).map_err(|e| miette::miette!("encode_frame body: {e}"))?,
    );
    Ok(bytes::Bytes::from(buf))
}

#[cfg(feature = "relay")]
pub fn encode_error_frame(error: &str, message: Option<&str>) -> miette::Result<bytes::Bytes> {
    let mut buf = serde_ipld_dagcbor::to_vec(&EncodeErrorHeader { op: -1 })
        .map_err(|e| miette::miette!("encode_error_frame header: {e}"))?;
    buf.extend_from_slice(
        &serde_ipld_dagcbor::to_vec(&EncodeErrorFrame { error, message })
            .map_err(|e| miette::miette!("encode_error_frame body: {e}"))?,
    );
    Ok(bytes::Bytes::from(buf))
}
