use std::collections::BTreeSet;
use smol_str::{SmolStr, ToSmolStr};
use miette::{IntoDiagnostic, Result, WrapErr};
use url::Url;

use crate::db::keys;
use crate::pds_meta::HostStatus;
use super::Hydrant;

#[derive(Debug, Clone)]
pub struct ApiBinds {
    pub(crate) head: std::net::SocketAddr,
    pub(crate) tail: Vec<std::net::SocketAddr>,
}

impl ApiBinds {
    pub fn new(addr: std::net::SocketAddr) -> Self {
        Self {
            head: addr,
            tail: Vec::new(),
        }
    }

    pub fn try_from_iter<I: IntoIterator<Item = std::net::SocketAddr>>(iter: I) -> Option<Self> {
        let mut iter = iter.into_iter();
        let head = iter.next()?;
        Some(Self {
            head,
            tail: iter.collect(),
        })
    }

    pub fn iter(&self) -> impl Iterator<Item = std::net::SocketAddr> + '_ {
        std::iter::once(self.head).chain(self.tail.iter().copied())
    }
}

#[derive(Debug, Clone)]
/// information about a host hydrant is consuming from.
pub struct Host {
    /// hostname of the host.
    pub name: SmolStr,
    /// latest seq hydrant has processed from this host.
    pub seq: i64,
    /// the amount of accounts hydrant has seen from this host.
    pub account_count: u64,
    /// the status of this host in hydrant.
    pub status: crate::pds_meta::HostStatus,
}

impl Hydrant {
    /// get the status of a (firehose) host we are consuming from.
    ///
    /// returns the seq we are on for this host.
    pub async fn get_host_status(&self, hostname: &str) -> Result<Option<Host>> {
        let state = self.state.clone();
        let hostname = hostname.to_smolstr();

        tokio::task::spawn_blocking(move || {
            let key = keys::firehose_cursor_key(&hostname);

            let mut seq = 0;
            if let Some(cursor_bytes) = state.db.cursors.get(&key).into_diagnostic()? {
                seq = i64::from_be_bytes(cursor_bytes.as_ref().try_into().into_diagnostic()?);
            } else {
                // if it has no cursor, check if it's explicitly tracked in hosts map
                // or firehose tasks (recently added via API but no messages yet)
                let meta = state.pds_meta.load();
                if !meta.hosts.contains_key(hostname.as_str()) {
                    // we should also allow it if it's an active firehose ingestor
                    let mut found_in_cursors = false;
                    state.firehose_cursors.iter_sync(|u, _| {
                        if u.host_str() == Some(hostname.as_str()) {
                            found_in_cursors = true;
                        }
                        !found_in_cursors // continue if not found
                    });

                    if !found_in_cursors {
                        return Ok(None);
                    }
                }
            }

            let account_count = state
                .db
                .get_count_sync(&keys::pds_account_count_key(&hostname));
            let status = state.pds_meta.load().status(&hostname);

            Ok(Some(Host {
                name: hostname,
                seq,
                account_count,
                status,
            }))
        })
        .await
        .into_diagnostic()?
    }

    /// enumerates all hosts hydrant is consuming from.
    ///
    /// returns hosts enumerated in this pagination and the cursor to paginate from.
    pub async fn list_hosts(
        &self,
        cursor: Option<&str>,
        limit: usize,
    ) -> Result<(Vec<Host>, Option<SmolStr>)> {
        let state = self.state.clone();
        let cursor = cursor.map(str::to_string);

        tokio::task::spawn_blocking(move || {
            let prefix_end = {
                let mut end = keys::FIREHOSE_CURSOR_PREFIX.to_vec();
                *end.last_mut().unwrap() += 1;
                end
            };

            let mut all_hostnames = BTreeSet::new();
            for item in state.db.cursors.range((
                std::ops::Bound::Included(keys::FIREHOSE_CURSOR_PREFIX.to_vec()),
                std::ops::Bound::Excluded(prefix_end),
            )) {
                let (k, _) = item.into_inner().into_diagnostic()?;
                let hostname = std::str::from_utf8(&k[keys::FIREHOSE_CURSOR_PREFIX.len()..])
                    .into_diagnostic()
                    .wrap_err("firehose cursor key contains non-utf8 hostname")?;
                all_hostnames.insert(SmolStr::new(hostname));
            }

            {
                let meta = state.pds_meta.load();
                for hostname in meta.hosts.keys() {
                    all_hostnames.insert(SmolStr::new(hostname));
                }
            }

            let hostnames = all_hostnames.into_iter().collect::<Vec<_>>();
            let start_idx = cursor
                .as_deref()
                .map(|after| hostnames.partition_point(|host| host.as_str() <= after))
                .unwrap_or(0);

            let selected = hostnames
                .iter()
                .skip(start_idx)
                .take(limit + 1)
                .cloned()
                .collect::<Vec<_>>();

            let mut hosts: Vec<Host> = Vec::with_capacity(selected.len().min(limit));
            for hostname in selected.iter().take(limit) {
                let seq = state
                    .db
                    .cursors
                    .get(keys::firehose_cursor_key(hostname))
                    .into_diagnostic()?
                    .map(|v| {
                        v.as_ref()
                            .try_into()
                            .into_diagnostic()
                            .wrap_err("cursor value is not 8 bytes")
                            .map(i64::from_be_bytes)
                    })
                    .transpose()?
                    .unwrap_or(0);
                let account_count = state
                    .db
                    .get_count_sync(&keys::pds_account_count_key(hostname));
                let status = state.pds_meta.load().status(hostname);
                hosts.push(Host {
                    name: hostname.clone(),
                    seq,
                    account_count,
                    status,
                });
            }

            let next_cursor = if selected.len() > limit {
                hosts.last().map(|h| h.name.clone())
            } else {
                None
            };

            Ok((hosts, next_cursor))
        })
        .await
        .into_diagnostic()?
    }
}

#[cfg(test)]
mod host_listing_tests {
    use super::*;
    use crate::config::Config;
    use crate::db::set_ks_count;
    use crate::pds_meta::HostStatus;

    fn test_config(path: &std::path::Path) -> Config {
        Config {
            database_path: path.to_path_buf(),
            ..Default::default()
        }
    }

    #[tokio::test]
    async fn list_hosts_includes_seeded_hosts_without_cursors() -> Result<()> {
        let tmp = tempfile::tempdir().into_diagnostic()?;
        let hydrant = Hydrant::new(test_config(tmp.path())).await?;

        {
            let state = hydrant.state.clone();
            tokio::task::spawn_blocking(move || -> Result<()> {
                let mut batch = state.db.inner.batch();
                crate::db::pds_meta::set_status(
                    &mut batch,
                    &state.db.filter,
                    "offline.example",
                    HostStatus::Offline,
                )?;
                crate::db::pds_meta::set_status(
                    &mut batch,
                    &state.db.filter,
                    "active.example",
                    HostStatus::Active,
                )?;
                set_ks_count(
                    &mut batch,
                    &state.db,
                    &keys::pds_account_count_key("offline.example"),
                    7,
                );
                set_ks_count(
                    &mut batch,
                    &state.db,
                    &keys::pds_account_count_key("active.example"),
                    42,
                );
                state
                    .db
                    .cursors
                    .insert(
                        keys::firehose_cursor_key("active.example"),
                        123_i64.to_be_bytes(),
                    )
                    .into_diagnostic()?;
                batch.commit().into_diagnostic()?;
                state.db.persist()
            })
            .await
            .into_diagnostic()??;

            crate::pds_meta::PdsMeta::update_host(
                &hydrant.state.pds_meta,
                "offline.example",
                |h| {
                    h.status = HostStatus::Offline;
                },
            );
            crate::pds_meta::PdsMeta::update_host(&hydrant.state.pds_meta, "active.example", |h| {
                h.status = HostStatus::Active;
            });
            hydrant.state.db.update_count("p|offline.example", 7);
            hydrant.state.db.update_count("p|active.example", 42);
        }

        let (hosts, next) = hydrant.list_hosts(None, 10).await?;
        assert!(next.is_none());

        let offline = hosts
            .iter()
            .find(|h| h.name == "offline.example")
            .expect("seeded host without cursor should be listed");
        let active = hosts
            .iter()
            .find(|h| h.name == "active.example")
            .expect("host with cursor should be listed");

        assert_eq!(offline.seq, 0);
        assert_eq!(offline.account_count, 7);
        assert_eq!(offline.status, HostStatus::Offline);

        assert_eq!(active.seq, 123);
        assert_eq!(active.account_count, 42);
        assert_eq!(active.status, HostStatus::Active);

        Ok(())
    }
}
