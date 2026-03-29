use tokio::sync::mpsc;
use tracing::warn;

pub mod firehose;
#[cfg(feature = "relay")]
pub mod relay_worker;
pub mod stream;
pub mod validation;
#[cfg(feature = "events")]
pub mod worker;

use jacquard_common::types::crypto::PublicKey;
use jacquard_common::types::did::Did;
use miette::Result;
use smol_str::{SmolStr, ToSmolStr};
use url::Url;

use crate::ingest::stream::{AccountStatus, SubscribeReposMessage};
use crate::resolver::Resolver;
use crate::types::{RepoState, RepoStatus};

#[derive(Debug)]
pub enum IngestMessage {
    Firehose {
        relay: Url,
        /// true when `relay` is a direct PDS connection (not an aggregating relay).
        /// enables host authority enforcement in the worker.
        is_pds: bool,
        msg: SubscribeReposMessage<'static>,
    },
    BackfillFinished(Did<'static>),
}

pub type BufferTx = mpsc::UnboundedSender<IngestMessage>;
pub type BufferRx = mpsc::UnboundedReceiver<IngestMessage>;

/// outcome of a host authority check.
enum AuthorityOutcome {
    /// stored pds matched the source host immediately.
    Authorized,
    /// pds migrated: doc now points to this host, but our stored state was stale.
    WasStale,
    /// host did not match even after doc resolution.
    WrongHost { expected: SmolStr },
}

fn pds_host(pds: Option<&str>) -> Option<SmolStr> {
    // todo: add faster host parsing since we only need that
    pds.and_then(|pds| Url::parse(pds).ok()).map(|u| {
        u.host_str()
            .map(SmolStr::new)
            .expect("that there is host in pds url")
    })
}

/// invalidates the resolver cache for `did`, fetches a fresh document, and updates `repo_state`.
///
/// panics if called outside a tokio runtime context.
fn refresh_doc(resolver: &Resolver, did: &Did, repo_state: &mut RepoState) -> Result<()> {
    resolver.invalidate_sync(did);
    let doc = tokio::runtime::Handle::current()
        .block_on(resolver.resolve_doc(did))
        .map_err(|e| miette::miette!("{e}"))?;
    repo_state.update_from_doc(doc);
    repo_state.touch();
    Ok(())
}

/// checks that `source_host` is the authoritative PDS for `did`.
///
/// updates `repo_state` in place when a doc refresh is performed (i.e. on any outcome other than
/// `Authorized`). callers that persist state (e.g. the indexer worker) should write `repo_state`
/// to their batch after this call when the outcome is not `Authorized`.
///
/// panics if called outside a tokio runtime context.
fn check_host_authority(
    resolver: &Resolver,
    did: &Did,
    repo_state: &mut RepoState,
    source_host: &str,
) -> Result<AuthorityOutcome> {
    let expected = pds_host(repo_state.pds.as_deref());
    if expected.as_deref() == Some(source_host) {
        return Ok(AuthorityOutcome::Authorized);
    }

    // try again once
    refresh_doc(resolver, did, repo_state)?;
    let Some(expected) = pds_host(repo_state.pds.as_deref()) else {
        miette::bail!("can't get pds host???");
    };
    if expected.as_str() == source_host {
        Ok(AuthorityOutcome::WasStale)
    } else {
        Ok(AuthorityOutcome::WrongHost { expected })
    }
}

/// resolves the signing key for `did` if `verify_signatures` is true.
///
/// panics if called outside a tokio runtime context.
fn fetch_key(
    resolver: &Resolver,
    verify_signatures: bool,
    did: &Did,
) -> Result<Option<PublicKey<'static>>> {
    if verify_signatures {
        let key = tokio::runtime::Handle::current()
            .block_on(resolver.resolve_signing_key(did))
            .map_err(|e| miette::miette!("{e}"))?;
        Ok(Some(key))
    } else {
        Ok(None)
    }
}

/// maps an inactive account status to the corresponding `RepoStatus`.
/// panics on `AccountStatus::Deleted`, caller must handle that
fn inactive_account_repo_status(did: &Did, status: &Option<AccountStatus<'_>>) -> RepoStatus {
    match status {
        Some(AccountStatus::Takendown) => RepoStatus::Takendown,
        Some(AccountStatus::Suspended) => RepoStatus::Suspended,
        Some(AccountStatus::Deactivated) => RepoStatus::Deactivated,
        Some(AccountStatus::Throttled) => RepoStatus::Error("throttled".into()),
        Some(AccountStatus::Desynchronized) => RepoStatus::Error("desynchronized".into()),
        Some(AccountStatus::Other(s)) => {
            warn!(did = %did, status = %s, "unknown account status");
            RepoStatus::Error(s.to_smolstr())
        }
        Some(AccountStatus::Deleted) => unreachable!("deleted is handled before status mapping"),
        None => {
            warn!(did = %did, "account inactive but no status provided");
            RepoStatus::Error("unknown".into())
        }
    }
}
