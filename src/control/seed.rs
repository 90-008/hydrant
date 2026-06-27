use std::sync::Arc;
use std::sync::atomic::{AtomicI64, Ordering};
use std::time::Duration;

use futures::StreamExt;
use jacquard_api::com_atproto::sync::HostStatus;
use jacquard_api::com_atproto::sync::list_hosts::{Host, ListHostsOutput};
use miette::{Context, IntoDiagnostic};
use tracing::{debug, info, warn};
use url::Url;

use super::firehose::FirehoseHandle;
use crate::db::keys;
use crate::state::AppState;

const MAX_CONCURRENT_SEEDS: usize = 4;
const LIST_HOSTS_BODY_PREVIEW_BYTES: usize = 512;

/// seed firehose pds sources by calling `com.atproto.sync.listHosts` on each seed URL.
/// banned pds' are not added, everything else is (including offline)
pub(crate) async fn seed_from_list_hosts(
    seed_urls: &[Url],
    firehose: &FirehoseHandle,
    state: &Arc<AppState>,
) {
    info!("will seed urls...");

    let http = seed_http_client();

    let mut futs = futures::stream::iter(seed_urls.iter().cloned())
        .map(|seed_url| {
            let firehose = Some(firehose.clone());
            let state = state.clone();
            let http = http.clone();
            async move { seed_one(&seed_url, firehose, &state, &http).await }
        })
        .buffer_unordered(MAX_CONCURRENT_SEEDS);

    while futs.next().await.is_some() {}
}

/// refresh seed relay host status/cursor snapshots without adding sources.
///
/// this runs before persisted sources are spawned so existing PDS tasks start
/// from the seed relay's current cursor instead of racing ahead with no cursor.
pub(crate) async fn refresh_seed_snapshots(seed_urls: &[Url], state: &Arc<AppState>) {
    info!("will refresh seed snapshots...");

    let http = seed_http_client();

    let mut futs = futures::stream::iter(seed_urls.iter().cloned())
        .map(|seed_url| {
            let state = state.clone();
            let http = http.clone();
            async move { seed_one(&seed_url, None, &state, &http).await }
        })
        .buffer_unordered(MAX_CONCURRENT_SEEDS);

    while futs.next().await.is_some() {}
}

fn seed_http_client() -> reqwest::Client {
    reqwest::Client::builder()
        .user_agent(concat!(
            env!("CARGO_PKG_NAME"),
            "/",
            env!("CARGO_PKG_VERSION")
        ))
        .timeout(Duration::from_secs(10))
        .build()
        .expect("that reqwest will build")
}

#[tracing::instrument(skip_all, fields(seed_url = %seed_url))]
async fn seed_one(
    seed_url: &Url,
    firehose: Option<FirehoseHandle>,
    state: &Arc<AppState>,
    http: &reqwest::Client,
) {
    // always start from the beginning. `listHosts` is small, and resuming from
    // an old completed-run cursor hides earlier hosts after a restart.
    let mut cursor: Option<String> = None;
    info!("seeding firehose sources from listHosts");

    let mut total = 0usize;
    let mut added = 0usize;

    let mut page_count = 0;
    loop {
        page_count += 1;
        if page_count > 100 {
            warn!("listHosts pagination limit (100) reached, stopping");
            break;
        }

        let url = list_hosts_url(seed_url, cursor.as_deref());
        let resp = match http.get(url.clone()).send().await {
            Ok(r) => r,
            Err(e) => {
                warn!(url = %url, err = %e, "failed to fetch listHosts, stopping");
                break;
            }
        };

        let status = resp.status();
        let bytes = match read_limited_response(resp, 10 * 1024 * 1024).await {
            Ok(b) => b,
            Err(e) => {
                warn!(url = %url, err = %e, "failed to read listHosts response, stopping");
                break;
            }
        };

        if !status.is_success() {
            let body = body_preview(&bytes);
            warn!(
                url = %url,
                status = %status,
                body = %body,
                "listHosts returned error status, stopping"
            );
            break;
        }

        let body: ListHostsOutput<'_> = match serde_json::from_slice(&bytes) {
            Ok(b) => b,
            Err(e) => {
                warn!(
                    url = %url,
                    err = %e,
                    body = %body_preview(&bytes),
                    "failed to parse listHosts response, stopping"
                );
                break;
            }
        };

        let next_cursor = body.cursor.as_deref().map(str::to_owned);
        total += body.hosts.len();
        let page_hosts = body.hosts.len();

        if let Err(e) = apply_seed_snapshot(state, &body.hosts) {
            warn!(err = %e, "failed to apply listHosts seed snapshot");
        }

        let mut banned = 0usize;
        let mut invalid = 0usize;
        let mut already_known = 0usize;
        let mut queued = 0usize;
        let mut page_added = 0usize;
        if let Some(firehose) = firehose.as_ref() {
            let mut seed_sources = Vec::with_capacity(body.hosts.len());
            for host in &body.hosts {
                // skip banned hosts; everything else (active, idle, offline, throttled) is included
                // since the firehose ingestor handles reconnection for transiently-unavailable hosts
                if matches!(host.status, Some(HostStatus::Banned)) {
                    banned += 1;
                    continue;
                }

                if !is_safe_seed_host(&host.hostname) {
                    invalid += 1;
                    warn!(hostname = %host.hostname, "unsafe/private hostname in listHosts response, skipping");
                    continue;
                }

                let wss_url_str = format!("wss://{}/", host.hostname);
                let wss_url = match Url::parse(&wss_url_str) {
                    Ok(u) => u,
                    Err(e) => {
                        invalid += 1;
                        warn!(hostname = %host.hostname, err = %e, "invalid hostname in listHosts response, skipping");
                        continue;
                    }
                };

                // skip sources that are already tracked; offline retries are handled separately
                if firehose.is_source_known(&wss_url) {
                    already_known += 1;
                    continue;
                }

                queued += 1;
                seed_sources.push(wss_url);
            }

            match firehose.add_seeded_sources(seed_sources).await {
                Ok(n) => {
                    page_added = n;
                    added += n;
                }
                Err(e) => {
                    warn!(err = %e, "failed to add seeded firehose sources");
                }
            }
        }

        info!(
            page_hosts,
            banned,
            invalid,
            already_known,
            queued,
            added = page_added,
            adding_sources = firehose.is_some(),
            next_cursor = ?next_cursor,
            "processed listHosts page"
        );

        cursor = next_cursor;

        if cursor.is_none() {
            break;
        }
    }

    info!(
        total,
        added, "finished seeding firehose sources from listHosts"
    );
}

fn body_preview(bytes: &[u8]) -> String {
    let len = bytes.len().min(LIST_HOSTS_BODY_PREVIEW_BYTES);
    let mut body = String::from_utf8_lossy(&bytes[..len]).into_owned();
    if bytes.len() > len {
        body.push_str("...");
    }
    body
}

fn list_hosts_url(base: &Url, cursor: Option<&str>) -> Url {
    let mut url = base.clone();
    url.set_path("/xrpc/com.atproto.sync.listHosts");
    {
        let mut pairs = url.query_pairs_mut();
        pairs.append_pair("limit", "1000");
        if let Some(c) = cursor {
            pairs.append_pair("cursor", c);
        }
    }
    url
}

fn apply_seed_snapshot(state: &Arc<AppState>, hosts: &[Host<'_>]) -> miette::Result<()> {
    let mut batch = state.db.inner.batch();
    let mut cursor_updates = Vec::with_capacity(hosts.len());
    let mut status_updates = Vec::with_capacity(hosts.len());

    for host in hosts {
        let hostname = host.hostname.as_ref();
        let status = map_seed_status(host.status.as_ref());

        crate::db::pds_meta::set_status(&mut batch, &state.db.filter, hostname, status)?;
        status_updates.push((hostname.to_string(), status));

        let Some(seq) = host
            .seq
            .and_then(|seq| i64::try_from(seq).ok())
            .filter(|seq| *seq > 0)
        else {
            continue;
        };

        let cursor_key = keys::firehose_cursor_key(hostname);
        let existing_seq = state
            .db
            .cursors
            .get(&cursor_key)
            .into_diagnostic()?
            .map(|bytes| {
                bytes
                    .as_ref()
                    .try_into()
                    .into_diagnostic()
                    .wrap_err("cursor value is not 8 bytes")
                    .map(i64::from_be_bytes)
            })
            .transpose()?
            .unwrap_or(0);
        if seq > existing_seq {
            batch.insert(&state.db.cursors, cursor_key, seq.to_be_bytes());
            cursor_updates.push((hostname.to_string(), seq));
        }
    }

    batch.commit().into_diagnostic()?;
    debug!(
        hosts = hosts.len(),
        status_updates = status_updates.len(),
        cursor_updates = cursor_updates.len(),
        "applied listHosts seed snapshot"
    );

    state.pds_meta.rcu(|meta| {
        let mut next = (**meta).clone();
        for (hostname, status) in &status_updates {
            next.update_host_entry(hostname, |entry| entry.status = *status);
        }
        next
    });
    for (hostname, seq) in cursor_updates {
        let Ok(url) = Url::parse(&format!("wss://{hostname}/")) else {
            continue;
        };
        let _ = state
            .firehose_cursors
            .insert_sync(url.clone(), AtomicI64::new(seq));
        state.firehose_cursors.peek_with(&url, |_, cursor| {
            if seq > cursor.load(Ordering::SeqCst) {
                cursor.store(seq, Ordering::SeqCst);
            }
        });
    }

    Ok(())
}

fn map_seed_status(status: Option<&HostStatus<'_>>) -> crate::pds_meta::HostStatus {
    match status {
        Some(HostStatus::Active) | None => crate::pds_meta::HostStatus::Active,
        Some(HostStatus::Idle) => crate::pds_meta::HostStatus::Idle,
        Some(HostStatus::Offline) => crate::pds_meta::HostStatus::Offline,
        Some(HostStatus::Throttled) => crate::pds_meta::HostStatus::Throttled,
        Some(HostStatus::Banned) => crate::pds_meta::HostStatus::Banned,
        Some(HostStatus::Other(_)) => crate::pds_meta::HostStatus::Active,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::Config;
    use jacquard_common::CowStr;
    use tempfile::tempdir;

    fn persisted_cursor(state: &AppState, hostname: &str) -> miette::Result<Option<i64>> {
        state
            .db
            .cursors
            .get(keys::firehose_cursor_key(hostname))
            .into_diagnostic()?
            .map(|bytes| {
                bytes
                    .as_ref()
                    .try_into()
                    .into_diagnostic()
                    .map(i64::from_be_bytes)
            })
            .transpose()
    }

    #[test]
    fn apply_seed_snapshot_persists_statuses_and_cursors() -> miette::Result<()> {
        let tmp = tempdir().into_diagnostic()?;
        let cfg = Config {
            database_path: tmp.path().to_path_buf(),
            ..Default::default()
        };
        let state = Arc::new(AppState::new(&cfg)?);

        let hosts = vec![
            Host {
                hostname: CowStr::Borrowed("active.example"),
                account_count: Some(42),
                seq: Some(100),
                status: Some(HostStatus::Active),
                extra_data: None,
            },
            Host {
                hostname: CowStr::Borrowed("offline.example"),
                account_count: Some(7),
                seq: Some(5),
                status: Some(HostStatus::Offline),
                extra_data: None,
            },
        ];

        apply_seed_snapshot(&state, &hosts)?;

        assert_eq!(
            state
                .db
                .get_count_sync(&keys::pds_account_count_key("active.example")),
            0
        );
        assert_eq!(
            state
                .db
                .get_count_sync(&keys::pds_account_count_key("offline.example")),
            0
        );

        let meta = state.pds_meta.load();
        assert_eq!(
            meta.status("active.example"),
            crate::pds_meta::HostStatus::Active
        );
        assert_eq!(
            meta.status("offline.example"),
            crate::pds_meta::HostStatus::Offline
        );
        assert!(meta.hosts.contains_key("active.example"));
        assert!(meta.hosts.contains_key("offline.example"));

        assert_eq!(persisted_cursor(&state, "active.example")?, Some(100));
        assert_eq!(persisted_cursor(&state, "offline.example")?, Some(5));

        let active_url = Url::parse("wss://active.example/").into_diagnostic()?;
        let in_memory = state
            .firehose_cursors
            .peek_with(&active_url, |_, cursor| cursor.load(Ordering::SeqCst));
        assert_eq!(in_memory, Some(100));

        Ok(())
    }

    #[test]
    fn apply_seed_snapshot_does_not_lower_existing_cursor() -> miette::Result<()> {
        let tmp = tempdir().into_diagnostic()?;
        let cfg = Config {
            database_path: tmp.path().to_path_buf(),
            ..Default::default()
        };
        let state = Arc::new(AppState::new(&cfg)?);
        let url = Url::parse("wss://active.example/").into_diagnostic()?;

        crate::db::set_firehose_cursor(&state.db, &url, 150)?;
        let _ = state
            .firehose_cursors
            .insert_sync(url.clone(), AtomicI64::new(150));

        let hosts = vec![Host {
            hostname: CowStr::Borrowed("active.example"),
            account_count: Some(42),
            seq: Some(100),
            status: Some(HostStatus::Active),
            extra_data: None,
        }];

        apply_seed_snapshot(&state, &hosts)?;

        assert_eq!(persisted_cursor(&state, "active.example")?, Some(150));
        let in_memory = state
            .firehose_cursors
            .peek_with(&url, |_, cursor| cursor.load(Ordering::SeqCst));
        assert_eq!(in_memory, Some(150));

        Ok(())
    }
}

async fn read_limited_response(resp: reqwest::Response, limit: usize) -> miette::Result<bytes::Bytes> {
    use bytes::BytesMut;
    use futures::StreamExt as _;
    let mut stream = resp.bytes_stream();
    let mut buf = BytesMut::new();
    while let Some(chunk) = stream.next().await {
        let chunk = chunk.into_diagnostic().context("failed to read response chunk")?;
        if buf.len() + chunk.len() > limit {
            miette::bail!("response body too large (exceeds {limit} bytes)");
        }
        buf.extend_from_slice(&chunk);
    }
    Ok(buf.freeze())
}

fn is_private_ip(ip: std::net::IpAddr) -> bool {
    match ip {
        std::net::IpAddr::V4(ip) => ip.is_loopback() || ip.is_private() || ip.is_link_local() || ip.is_multicast() || ip.is_unspecified(),
        std::net::IpAddr::V6(ip) => {
            ip.is_loopback() || ip.is_multicast() || ip.is_unspecified() || {
                let octets = ip.octets();
                (octets[0] & 0xfe) == 0xfc || (octets[0] == 0xfe && (octets[1] & 0xc0) == 0x80)
            }
        }
    }
}

fn is_safe_seed_host(host: &str) -> bool {
    if host.is_empty() {
        return false;
    }
    if host == "localhost" || host.ends_with(".local") || host.ends_with(".internal") {
        return false;
    }
    if let Ok(ip) = host.parse::<std::net::IpAddr>() {
        return !is_private_ip(ip);
    }
    true
}

