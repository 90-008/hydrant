use jacquard_common::IntoStatic;
use jacquard_common::types::crypto::PublicKey;
use jacquard_repo::MemoryBlockStore;
use jacquard_repo::Mst;
use jacquard_repo::car::reader::{ParsedCar, parse_car_bytes};
use jacquard_repo::commit::Commit as AtpCommit;
use jacquard_repo::mst::VerifiedWriteOp;
use miette::IntoDiagnostic;
use smol_str::ToSmolStr;
use std::sync::Arc;
use thiserror::Error;

use crate::ingest::stream::{Commit, RepoOpAction, Sync};
use crate::types::RepoState;

/// describes which size limit was exceeded
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SizeLimitKind {
    /// msg.blocks.len() > 2 MiB
    BlocksField,
    /// msg.ops.len() > 200
    OpCount,
    /// individual record block > 1 MiB
    RecordSize,
}

impl std::fmt::Display for SizeLimitKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::BlocksField => write!(f, "blocks field exceeds 2MiB"),
            Self::OpCount => write!(f, "op count exceeds 200"),
            Self::RecordSize => write!(f, "record block exceeds 1MiB"),
        }
    }
}

#[derive(Debug, Error)]
pub enum CommitValidationError {
    /// rev is not greater than the last known rev
    #[error("stale rev")]
    StaleRev,
    /// rev timestamp exceeds the clock skew window
    #[error("future rev")]
    FutureRev,
    /// CAR could not be parsed, or a required block is missing
    #[error("malformed CAR: {0}")]
    MalformedCar(miette::Report),
    /// wire message fields differ from the signed commit object
    #[error("field mismatch in {field}")]
    FieldMismatch { field: &'static str },
    /// signature verification failed.
    #[error("signature verification failed")]
    SigFailure,
    /// a block, op count, or record exceeds the ATProto size limits
    #[error("size limit exceeded: {0}")]
    SizeLimitExceeded(SizeLimitKind),
    /// MST inversion check failed (only when verify_mst = true)
    #[error("MST inversion failed: {0}")]
    MstInvalid(miette::Report),
    /// commit arrived from a host that is not the authoritative PDS for this DID
    /// (enforced in phase 2 relay worker)
    #[allow(dead_code)]
    #[error("commit from wrong host")]
    WrongHost,
}

#[derive(Debug, Error)]
pub enum SyncValidationError {
    /// blocks field exceeds 2MiB
    #[error("size limit exceeded")]
    SizeLimitExceeded,
    /// CAR could not be parsed
    #[error("malformed CAR: {0}")]
    MalformedCar(miette::Report),
    /// wire message fields differ from the signed commit object
    #[error("field mismatch in {field}")]
    FieldMismatch { field: &'static str },
    /// signature verification failed
    #[error("signature verification failed")]
    SigFailure,
}

/// indicates that the commit's chain pointers do not match the last known repo state.
/// this is not a hard rejection so callers can decide whta they want to do
#[derive(Default, Debug)]
pub struct ChainBreak {
    /// msg.since is present and does not match the last known rev
    pub since_mismatch: bool,
    /// msg.prev_data does not match the last known data CID
    pub prev_data_mismatch: bool,
}

impl ChainBreak {
    pub fn is_broken(&self) -> bool {
        self.since_mismatch || self.prev_data_mismatch
    }
}

/// a successfully validated `#commit` message, carrying pre-parsed data for apply_commit
pub struct ValidatedCommit<'c> {
    pub commit: &'c Commit<'c>,
    /// result of parse_car_bytes, already done so apply_commit does not re-parse
    pub parsed_blocks: ParsedCar,
    pub commit_obj: AtpCommit<'static>,
    pub chain_break: ChainBreak,
}

/// a successfully validated `#sync` message
pub struct ValidatedSync {
    pub commit_obj: AtpCommit<'static>,
}

pub struct ValidationOptions {
    /// clock drift window for future-rev rejection (seconds). default: 300
    pub rev_clock_skew_secs: i64,
    /// run MST inversion validation (expensive). default: false
    pub verify_mst: bool,
}

impl Default for ValidationOptions {
    fn default() -> Self {
        Self {
            rev_clock_skew_secs: 300,
            verify_mst: false,
        }
    }
}

/// all methods panic if called outside a tokio runtime context.
pub struct ValidationContext<'a> {
    pub opts: &'a ValidationOptions,
}

impl ValidationContext<'_> {
    pub fn validate_commit<'c>(
        &self,
        msg: &'c Commit<'c>,
        repo_state: &RepoState,
        signing_key: Option<&PublicKey>,
    ) -> Result<ValidatedCommit<'c>, CommitValidationError> {
        validate_commit(msg, repo_state, signing_key, self.opts)
    }

    pub fn validate_sync(
        &self,
        msg: &Sync<'_>,
        signing_key: Option<&PublicKey>,
    ) -> Result<ValidatedSync, SyncValidationError> {
        validate_sync(msg, signing_key)
    }
}

/// validate an incoming `#commit` message.
///
/// on success, returns a `ValidatedCommit` carrying pre-parsed data so that
/// `apply_commit` does not need to repeat the work.
///
/// chain-break (since/prevData mismatch) is NOT an error. callers check
/// `validated.chain_break.is_some()` and decide how to respond.
///
/// - `signing_key`: `None` when signature verification is disabled.
///
/// panics if called outside a tokio runtime context.
pub fn validate_commit<'c>(
    msg: &'c Commit<'c>,
    repo_state: &RepoState,
    signing_key: Option<&PublicKey>,
    opts: &ValidationOptions,
) -> Result<ValidatedCommit<'c>, CommitValidationError> {
    let handle = tokio::runtime::Handle::current();
    const MAX_BLOCKS_BYTES: usize = 2_097_152; // 2 MiB
    const MAX_OPS: usize = 200;
    const MAX_RECORD_BYTES: usize = 1_048_576; // 1 MiB

    // 1. size limits
    if msg.blocks.len() > MAX_BLOCKS_BYTES {
        return Err(CommitValidationError::SizeLimitExceeded(
            SizeLimitKind::BlocksField,
        ));
    }
    if msg.ops.len() > MAX_OPS {
        return Err(CommitValidationError::SizeLimitExceeded(
            SizeLimitKind::OpCount,
        ));
    }

    // 2. stale rev, skip if msg.rev <= last known rev (lexicographic order)
    if let Some(root) = &repo_state.root {
        if msg.rev.as_str() <= root.rev.to_tid().as_str() {
            return Err(CommitValidationError::StaleRev);
        }
    }

    // 3. future rev, reject if rev timestamp is more than clock_skew_secs ahead of now
    {
        let rev_us = msg.rev.timestamp() as i64;
        let now_us = chrono::Utc::now().timestamp_micros();
        if rev_us > now_us + opts.rev_clock_skew_secs * 1_000_000 {
            return Err(CommitValidationError::FutureRev);
        }
    }

    // 4. CAR parse
    let parsed = handle
        .block_on(parse_car_bytes(msg.blocks.as_ref()))
        .map_err(|e| CommitValidationError::MalformedCar(miette::miette!("{e}")))?;

    let root_bytes = parsed.blocks.get(&parsed.root).ok_or_else(|| {
        CommitValidationError::MalformedCar(miette::miette!("root block missing from CAR"))
    })?;

    // 5. commit object deserialization
    let commit_obj = AtpCommit::from_cbor(root_bytes)
        .map_err(|e| CommitValidationError::MalformedCar(miette::miette!("{e}")))?;

    // 6. field consistency: wire message vs signed commit object
    if commit_obj.did.as_str() != msg.repo.as_str() {
        return Err(CommitValidationError::FieldMismatch { field: "repo" });
    }
    if commit_obj.rev.as_str() != msg.rev.as_str() {
        return Err(CommitValidationError::FieldMismatch { field: "rev" });
    }

    // 7. signature verification
    if let Some(key) = signing_key {
        commit_obj
            .verify(key)
            .map_err(|_| CommitValidationError::SigFailure)?;
    }

    let commit_obj = commit_obj.into_static();

    // 8. chain break checks
    let chain_break = repo_state
        .root
        .as_ref()
        .map(|r| breaks_chain(msg, r))
        .unwrap_or_default();

    // 9–10. per-record size limits and basic CBOR validity
    for op in &msg.ops {
        let Some(cid_link) = &op.cid else { continue };
        let cid = cid_link.to_ipld().map_err(|e| {
            CommitValidationError::MalformedCar(miette::miette!("invalid op CID: {e}"))
        })?;
        let Some(block) = parsed.blocks.get(&cid) else {
            return Err(CommitValidationError::MalformedCar(miette::miette!(
                "block for op CID {cid} missing from CAR"
            )));
        };

        if block.len() > MAX_RECORD_BYTES {
            return Err(CommitValidationError::SizeLimitExceeded(
                SizeLimitKind::RecordSize,
            ));
        }

        serde_ipld_dagcbor::from_slice::<jacquard_common::Data>(block).map_err(|e| {
            CommitValidationError::MalformedCar(miette::miette!("record is not valid CBOR: {e}"))
        })?;
    }

    // 11. MST inversion
    if opts.verify_mst {
        verify_mst(msg, &parsed, &commit_obj, &handle)
            .map_err(CommitValidationError::MstInvalid)?;
    }

    Ok(ValidatedCommit {
        commit: msg,
        parsed_blocks: parsed,
        commit_obj,
        chain_break,
    })
}

/// panics if called outside a tokio runtime context.
pub fn validate_sync<'c>(
    msg: &'c Sync<'c>,
    signing_key: Option<&PublicKey>,
) -> Result<ValidatedSync, SyncValidationError> {
    let handle = tokio::runtime::Handle::current();
    const MAX_BLOCKS_BYTES: usize = 2_097_152;

    // 1. size limit
    if msg.blocks.len() > MAX_BLOCKS_BYTES {
        return Err(SyncValidationError::SizeLimitExceeded);
    }

    // 2. CAR parse
    let parsed = handle
        .block_on(parse_car_bytes(msg.blocks.as_ref()))
        .map_err(|e| SyncValidationError::MalformedCar(miette::miette!("{e}")))?;

    let root_bytes = parsed.blocks.get(&parsed.root).ok_or_else(|| {
        SyncValidationError::MalformedCar(miette::miette!("root block missing from CAR"))
    })?;

    // 3. commit object deserialization
    let commit_obj = AtpCommit::from_cbor(root_bytes)
        .map_err(|e| SyncValidationError::MalformedCar(miette::miette!("{e}")))?;

    // 4. field consistency
    if commit_obj.did.as_str() != msg.did.as_str() {
        return Err(SyncValidationError::FieldMismatch { field: "did" });
    }
    if commit_obj.rev.as_str() != msg.rev.as_str() {
        return Err(SyncValidationError::FieldMismatch { field: "rev" });
    }

    // 5. signature verification
    if let Some(key) = signing_key {
        commit_obj
            .verify(key)
            .map_err(|_| SyncValidationError::SigFailure)?;
    }

    Ok(ValidatedSync {
        commit_obj: commit_obj.into_static(),
    })
}

fn breaks_chain(msg: &Commit<'_>, root: &crate::types::Commit) -> ChainBreak {
    // since should equal the rev of the previous commit; only flag when since is present and wrong
    let since_mismatch = msg
        .since
        .as_ref()
        .map(|since| since.as_str() != root.rev.to_tid().as_str())
        .unwrap_or(false);

    // prev_data must equal the last known data CID when both are present
    let prev_data_mismatch = match &msg.prev_data {
        Some(prev_link) => match prev_link.to_ipld() {
            Ok(cid) => cid != root.data,
            Err(_) => true, // unparseable CID is a mismatch
        },
        None => true, // no prev_data but we have a previous state is a chain break
    };

    ChainBreak {
        since_mismatch,
        prev_data_mismatch,
    }
}

/// apply the inverse of each op (in reverse order) to the new MST and verify the resulting root
/// equals `msg.prev_data`. called only when `opts.verify_mst` is true.
fn verify_mst(
    msg: &Commit<'_>,
    parsed: &ParsedCar,
    commit_obj: &AtpCommit<'_>,
    handle: &tokio::runtime::Handle,
) -> miette::Result<()> {
    let store = Arc::new(MemoryBlockStore::new_from_blocks(parsed.blocks.clone()));
    let mut mst: Mst<MemoryBlockStore> = Mst::load(store, commit_obj.data, None);

    handle.block_on(async {
        for op in msg.ops.iter().rev() {
            let inv = match &op.action {
                RepoOpAction::Create => {
                    let cid_link = op
                        .cid
                        .as_ref()
                        .ok_or_else(|| miette::miette!("create op missing CID"))?;
                    let cid = cid_link.to_ipld().into_diagnostic()?;
                    VerifiedWriteOp::Create {
                        key: op.path.to_smolstr(),
                        cid,
                    }
                }
                RepoOpAction::Update => {
                    let cid_link = op
                        .cid
                        .as_ref()
                        .ok_or_else(|| miette::miette!("update op missing CID"))?;
                    let Some(prev_link) = op.prev.as_ref() else {
                        // prev is optional in inductive firehose (v3); skip if absent
                        continue;
                    };
                    let cid = cid_link.to_ipld().into_diagnostic()?;
                    let prev = prev_link.to_ipld().into_diagnostic()?;
                    VerifiedWriteOp::Update {
                        key: op.path.to_smolstr(),
                        cid,
                        prev,
                    }
                }
                RepoOpAction::Delete => {
                    let Some(prev_link) = op.prev.as_ref() else {
                        // prev is optional in inductive firehose (v3); skip if absent
                        continue;
                    };
                    let prev = prev_link.to_ipld().into_diagnostic()?;
                    VerifiedWriteOp::Delete {
                        key: op.path.to_smolstr(),
                        prev,
                    }
                }
                RepoOpAction::Other(action) => {
                    return Err(miette::miette!("unknown op action: {action}"));
                }
            };

            let ok = mst.invert_op(inv).await.into_diagnostic()?;
            if !ok {
                return Err(miette::miette!(
                    "MST inversion inconsistent with tree state for op on {}",
                    op.path
                ));
            }
        }

        // verify the resulting root CID equals prev_data (skip for genesis commits)
        if let Some(prev_link) = &msg.prev_data {
            let expected = prev_link.to_ipld().into_diagnostic()?;
            let root_cid = mst.get_pointer().await.into_diagnostic()?;
            if root_cid != expected {
                return Err(miette::miette!(
                    "MST inversion root mismatch: expected {expected}, got {root_cid}"
                ));
            }
        }

        Ok(())
    })
}
