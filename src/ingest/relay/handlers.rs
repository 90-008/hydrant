use miette::Result;
use smol_str::SmolStr;
#[cfg(all(feature = "relay", feature = "jetstream"))]
use smol_str::ToSmolStr;
use tokio::runtime::Handle;
use tracing::{debug, warn};
use url::Url;

use crate::db::{self, keys};
use crate::types::{RepoState, RepoStatus};

#[cfg(all(feature = "relay", feature = "jetstream"))]
use crate::db::types::TrimmedDid;
#[cfg(feature = "relay")]
use crate::ingest::stream::encode_frame;
use crate::ingest::stream::{Account, AccountStatus, Commit, Identity, Sync};
use crate::ingest::validation::ValidatedCommit;
#[cfg(feature = "indexer")]
use jacquard_common::IntoStatic;
#[cfg(all(feature = "relay", feature = "jetstream"))]
use crate::types::StoredJetstreamEvent;
#[cfg(all(feature = "relay", feature = "jetstream"))]
use jacquard_common::CowStr;

use super::{RelayWorker, WorkerContext};

impl RelayWorker {
    pub(crate) fn handle_commit(
        ctx: &mut WorkerContext,
        repo_state: &mut RepoState,
        #[allow(unused_variables)] firehose: &Url,
        #[allow(unused_mut)] mut commit: Commit<'static>,
    ) -> Result<()> {
        if !repo_state.active {
            return Ok(());
        }

        repo_state.advance_message_time(commit.time.0.timestamp_millis());

        let Some(validated) = ctx.validate_commit(repo_state, &commit)? else {
            return Ok(());
        };
        let ValidatedCommit {
            chain_break,
            commit_obj,
            parsed_blocks,
            ..
        } = validated;

        #[cfg(not(feature = "indexer"))]
        let _ = parsed_blocks;

        if chain_break.is_broken() {
            // chain breaks are not grounds for blocking when acting as a relay
            debug!(broken = ?chain_break, "chain break, forwarding anyway");
        }

        let repo_key = keys::repo_key(&commit.repo);

        #[cfg(feature = "indexer")]
        {
            ctx.pending_hook_messages
                .push(crate::ingest::indexer::IndexerMessage::Event(Box::new(
                    crate::ingest::indexer::IndexerEvent {
                        seq: commit.seq,
                        firehose: firehose.clone(),
                        data: crate::ingest::indexer::IndexerEventData::Commit(
                            crate::ingest::indexer::IndexerCommitData {
                                commit,
                                chain_break: chain_break.is_broken(),
                                parsed_blocks,
                            },
                        ),
                    },
                )));
        }
        #[cfg(feature = "relay")]
        {
            #[cfg(feature = "jetstream")]
            let jetstream_ops = commit
                .ops
                .iter()
                .enumerate()
                .filter_map(|(idx, op)| {
                    matches!(op.action.as_str(), "create" | "update" | "delete")
                        .then(|| split_collection(&op.path).map(|col| (idx as u32, col)))
                        .flatten()
                })
                .collect::<Vec<_>>();
            #[cfg(feature = "jetstream")]
            let jetstream_did = TrimmedDid::from(&commit.repo).into_static();

            let _relay_seq = ctx.queue_emit(|seq| {
                commit.seq = seq;
                encode_frame("#commit", &commit)
            })?;
            #[cfg(feature = "jetstream")]
            {
                // skip car parse and record materialization when no jetstream subscribers are
                // connected — the stream thread re-reads from relay_events as a fallback.
                let parsed_car = (ctx.state.db.jetstream_tx.receiver_count() > 0)
                    .then(|| {
                        tokio::runtime::Handle::current()
                            .block_on(jacquard_repo::car::reader::parse_car_bytes(
                                commit.blocks.as_ref(),
                            ))
                            .ok()
                    })
                    .flatten();
                for (op_index, collection) in &jetstream_ops {
                    let ephemeral = parsed_car.as_ref().and_then(|car| {
                        build_relay_commit_ephemeral(&commit, *op_index, collection, car)
                    });
                    ctx.pending_jetstream_events.push((
                        StoredJetstreamEvent::RelayCommit {
                            did: jetstream_did.clone(),
                            collection: collection.clone(),
                            relay_seq: _relay_seq,
                            op_index: *op_index,
                        },
                        ephemeral,
                    ));
                }
            }
        }

        repo_state.root = Some(commit_obj.into());
        repo_state.touch();
        ctx.batch.insert(
            &ctx.state.db.repos,
            repo_key,
            db::ser_repo_state(repo_state)?,
        );

        Ok(())
    }

    pub(crate) fn handle_sync(
        ctx: &mut WorkerContext,
        repo_state: &mut RepoState,
        #[allow(unused_variables)] firehose: &Url,
        #[allow(unused_mut)] mut sync: Sync<'static>,
    ) -> Result<()> {
        if !repo_state.active {
            return Ok(());
        }

        repo_state.advance_message_time(sync.time.0.timestamp_millis());

        let Some(validated) = ctx.validate_sync(repo_state, &sync)? else {
            return Ok(());
        };

        let repo_key = keys::repo_key(&sync.did);

        #[cfg(feature = "indexer")]
        {
            ctx.pending_hook_messages
                .push(crate::ingest::indexer::IndexerMessage::Event(Box::new(
                    crate::ingest::indexer::IndexerEvent {
                        seq: sync.seq,
                        firehose: firehose.clone(),
                        data: crate::ingest::indexer::IndexerEventData::Sync(
                            sync.did.into_static(),
                        ),
                    },
                )));
        }
        #[cfg(feature = "relay")]
        {
            ctx.queue_emit(|seq| {
                sync.seq = seq;
                encode_frame("#sync", &sync)
            })?;
        }

        repo_state.root = Some(validated.commit_obj.into());
        repo_state.touch();
        ctx.batch.insert(
            &ctx.state.db.repos,
            repo_key,
            db::ser_repo_state(repo_state)?,
        );

        Ok(())
    }

    pub(crate) fn handle_identity(
        ctx: &mut WorkerContext,
        repo_state: &mut RepoState,
        #[allow(unused_variables)] firehose: &Url,
        mut identity: Identity<'static>,
        is_pds: bool,
    ) -> Result<()> {
        let event_ms = identity.time.0.timestamp_millis();
        if !repo_state.should_process_identity_time(event_ms) {
            debug!("skipping stale/duplicate identity event");
            return Ok(());
        }
        repo_state.advance_identity_time(event_ms);
        let was_active = repo_state.active;
        let was_pds_host = Self::pds_host(repo_state.pds.as_deref());

        #[cfg(feature = "indexer")]
        let (was_handle, was_signing_key) = (
            repo_state.handle.clone().map(IntoStatic::into_static),
            repo_state.signing_key.clone().map(IntoStatic::into_static),
        );

        // refresh did doc if a pds sent this event
        // or if there is no handle specified
        if is_pds || identity.handle.is_none() {
            ctx.state.resolver.invalidate_sync(&identity.did);
            #[cfg(feature = "firehose-diagnostics")]
            let resolve_started = std::time::Instant::now();
            let doc = Handle::current().block_on(ctx.state.resolver.resolve_doc(&identity.did));
            #[cfg(feature = "firehose-diagnostics")]
            ctx.stats.record_resolve_doc(resolve_started.elapsed());
            match doc {
                Ok(doc) => {
                    repo_state.update_from_doc(doc);
                }
                Err(err) => {
                    warn!(err = %err, "couldnt fetch identity");
                }
            }
        }

        // don't pass handle through if it doesnt match ours for pds events
        if is_pds && repo_state.handle != identity.handle {
            identity.handle = None;
        }

        Self::update_pds_account_count(
            ctx,
            was_active,
            was_pds_host.as_deref(),
            repo_state.active,
            Self::pds_host(repo_state.pds.as_deref()).as_deref(),
        );

        let repo_key = keys::repo_key(&identity.did);

        #[cfg(feature = "indexer")]
        {
            let changed =
                repo_state.handle != was_handle || repo_state.signing_key != was_signing_key;
            ctx.pending_hook_messages
                .push(crate::ingest::indexer::IndexerMessage::Event(Box::new(
                    crate::ingest::indexer::IndexerEvent {
                        seq: identity.seq,
                        firehose: firehose.clone(),
                        data: crate::ingest::indexer::IndexerEventData::Identity(
                            crate::ingest::indexer::IndexerIdentityData { identity, changed },
                        ),
                    },
                )));
        }
        #[cfg(feature = "relay")]
        {
            let _relay_seq = ctx.queue_emit(|seq| {
                identity.seq = seq;
                encode_frame("#identity", &identity)
            })?;
            #[cfg(feature = "jetstream")]
            ctx.pending_jetstream_events.push((
                StoredJetstreamEvent::RelayIdentity {
                    did: TrimmedDid::from(&identity.did).into_static(),
                    relay_seq: _relay_seq,
                },
                None,
            ));
        }

        ctx.batch.insert(
            &ctx.state.db.repos,
            repo_key,
            db::ser_repo_state(repo_state)?,
        );

        Ok(())
    }

    pub(crate) fn handle_account(
        ctx: &mut WorkerContext,
        repo_state: &mut RepoState,
        #[allow(unused_variables)] firehose: &Url,
        #[allow(unused_mut)] mut account: Account<'static>,
        _is_pds: bool,
    ) -> Result<()> {
        let event_ms = account.time.0.timestamp_millis();
        if !repo_state.should_process_account_time(event_ms) {
            debug!("skipping stale/duplicate account event");
            return Ok(());
        }

        repo_state.advance_account_time(event_ms);

        // always capture was_active for count tracking, not just in indexer mode
        let was_active = repo_state.active;
        let was_pds_host = Self::pds_host(repo_state.pds.as_deref());
        #[cfg(feature = "indexer")]
        let was_status = repo_state.status.clone();

        repo_state.active = account.active;
        if !account.active {
            match &account.status {
                Some(AccountStatus::Deleted) => {
                    // keep a Deleted tombstone so any stale commits that arrive later
                    // (e.g. from the upstream backfill window) are not forwarded.
                    // per spec: "if any further #commit messages are emitted for the repo,
                    // all downstream services should ignore the event and not pass it through."
                    repo_state.status = RepoStatus::Deleted;
                }
                status => {
                    repo_state.status = ctx.inactive_account_repo_status(&account.did, status);
                }
            }
        } else {
            // active=true: desynchronized/throttled may still carry active=true per spec.
            // anything else (including unknown statuses) is treated as synced.
            repo_state.status = match &account.status {
                Some(AccountStatus::Desynchronized) => RepoStatus::Desynchronized,
                Some(AccountStatus::Throttled) => RepoStatus::Throttled,
                _ => RepoStatus::Synced,
            };
        }

        Self::update_pds_account_count(
            ctx,
            was_active,
            was_pds_host.as_deref(),
            repo_state.active,
            Self::pds_host(repo_state.pds.as_deref()).as_deref(),
        );

        let repo_key = keys::repo_key(&account.did);

        #[cfg(feature = "indexer")]
        {
            let changed = repo_state.active != was_active || repo_state.status != was_status;
            ctx.pending_hook_messages
                .push(crate::ingest::indexer::IndexerMessage::Event(Box::new(
                    crate::ingest::indexer::IndexerEvent {
                        seq: account.seq,
                        firehose: firehose.clone(),
                        data: crate::ingest::indexer::IndexerEventData::Account(
                            crate::ingest::indexer::IndexerAccountData {
                                account,
                                was_active,
                                changed,
                            },
                        ),
                    },
                )));
        }
        #[cfg(feature = "relay")]
        {
            let _relay_seq = ctx.queue_emit(|seq| {
                account.seq = seq;
                encode_frame("#account", &account)
            })?;
            #[cfg(feature = "jetstream")]
            ctx.pending_jetstream_events.push((
                StoredJetstreamEvent::RelayAccount {
                    did: TrimmedDid::from(&account.did).into_static(),
                    relay_seq: _relay_seq,
                },
                None,
            ));
        }

        repo_state.touch();
        ctx.batch.insert(
            &ctx.state.db.repos,
            repo_key,
            db::ser_repo_state(repo_state)?,
        );

        Ok(())
    }

    pub(super) fn pds_host(pds: Option<&str>) -> Option<SmolStr> {
        pds.and_then(|pds| Url::parse(pds).ok())
            .and_then(|url| url.host_str().map(SmolStr::new))
    }

    pub(super) fn update_pds_account_count(
        ctx: &mut WorkerContext,
        old_active: bool,
        old_host: Option<&str>,
        new_active: bool,
        new_host: Option<&str>,
    ) {
        if old_active && old_host == new_host && new_active {
            return;
        }

        let mut update_host = |host: &str, delta| {
            ctx.count_deltas.add_pds_account(host, delta);
            let count = ctx
                .count_deltas
                .projected_pds_account_count(&ctx.state.db, host);
            ctx.state
                .apply_host_limit_status(&mut ctx.batch, host, count);
        };

        if old_active && let Some(host) = old_host {
            update_host(host, -1);
        }

        if new_active && let Some(host) = new_host {
            update_host(host, 1);
        }
    }
}

#[cfg(all(feature = "relay", feature = "jetstream"))]
fn split_collection(path: &str) -> Option<CowStr<'static>> {
    path.split_once('/')
        .map(|(collection, _)| CowStr::Owned(collection.to_smolstr()))
}

#[cfg(all(feature = "relay", feature = "jetstream"))]
fn build_relay_commit_ephemeral(
    commit: &crate::ingest::stream::Commit,
    op_index: u32,
    collection: &jacquard_common::CowStr<'static>,
    car: &jacquard_repo::car::reader::ParsedCar,
) -> Option<crate::jetstream::JetstreamEphemeral> {
    let op = commit.ops.get(op_index as usize)?;
    let (_, rkey) = op.path.split_once('/')?;
    let action = op.action.as_str();

    let (record, cid) = if matches!(action, "create" | "update") {
        let cid_link = op.cid.as_ref()?;
        let cid_ipld = cid_link.to_ipld().ok()?;
        let block = car.blocks.get(&cid_ipld)?;
        let val = serde_ipld_dagcbor::from_slice::<jacquard_common::RawData>(block).ok()?;
        let record = serde_json::value::to_raw_value(&val)
            .ok()
            .map(std::sync::Arc::from);
        (record, Some(cid_link.to_string()))
    } else {
        (None, None)
    };

    Some(crate::jetstream::JetstreamEphemeral {
        did: commit.repo.as_str().to_string(),
        rev: commit.rev.as_str().to_string(),
        operation: action.to_string(),
        collection: collection.as_str().to_string(),
        rkey: rkey.to_string(),
        record,
        cid,
        live: true,
    })
}
