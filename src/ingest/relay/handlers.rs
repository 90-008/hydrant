use miette::Result;
use smol_str::SmolStr;
use tokio::runtime::Handle;
use tracing::{debug, warn};
use url::Url;

use crate::db::{self, keys};
use crate::types::{RepoState, RepoStatus};

use crate::ingest::stream::{Account, AccountStatus, Commit, Identity, Sync};
use crate::ingest::validation::ValidatedCommit;

use super::{RelayWorker, WorkerContext};

impl RelayWorker {
    pub(crate) fn handle_commit(
        ctx: &mut WorkerContext,
        repo_state: &mut RepoState,
        firehose: &Url,
        commit: Commit<'static>,
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

        if chain_break.is_broken() {
            // chain breaks are not grounds for blocking when acting as a relay
            debug!(broken = ?chain_break, "chain break, forwarding anyway");
        }

        let repo_key = keys::repo_key(&commit.repo);

        ctx.sink.commit(
            ctx.state,
            &mut ctx.batch,
            firehose,
            commit,
            chain_break.is_broken(),
            parsed_blocks,
        )?;

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
        firehose: &Url,
        sync: Sync<'static>,
    ) -> Result<()> {
        if !repo_state.active {
            return Ok(());
        }

        repo_state.advance_message_time(sync.time.0.timestamp_millis());

        let Some(validated) = ctx.validate_sync(repo_state, &sync)? else {
            return Ok(());
        };

        let repo_key = keys::repo_key(&sync.did);

        ctx.sink.sync(ctx.state, &mut ctx.batch, firehose, sync)?;

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
        firehose: &Url,
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
        let snapshot = ctx.sink.identity_snapshot(repo_state);

        // refresh did doc if a pds sent this event
        // or if there is no handle specified
        if is_pds || identity.handle.is_none() {
            ctx.state.resolver.invalidate_sync(&identity.did);
            let resolve_started = crate::ingest::firehose_stats::StatsInstant::now();
            let doc = Handle::current().block_on(ctx.state.resolver.resolve_doc(&identity.did));
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

        ctx.sink
            .identity(ctx.state, &mut ctx.batch, firehose, identity, repo_state, snapshot)?;

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
        firehose: &Url,
        account: Account<'static>,
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
        let snapshot = ctx.sink.account_snapshot(repo_state);

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

        ctx.sink.account(
            ctx.state,
            &mut ctx.batch,
            firehose,
            account,
            repo_state,
            snapshot,
            was_active,
        )?;

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
