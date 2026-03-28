use fjall::OwnedWriteBatch;
use miette::{IntoDiagnostic, Result};

use crate::db::{Db, FirehoseSourceMeta, keys};

// for this migration, we default to everything being a PDS unless it matches
// one of the hosts mentioned in this list. this is best effort but its
// better than defaulting to false and entirely disabling validation,
// the user can manage the firehose sources when they see the error anyway.
// (and i like to be correct :P)
const KNOWN_RELAY_HOSTS: &[&str] = &[
    "atproto.africa",
    "bsky.network",
    "relay1.us-east.bsky.network",
    "relay1.us-west.bsky.network",
    "relay.fire.hose.cam",
    "relay3.fr.hose.cam",
    "relay.upcloud.world",
    "relay.hayescmd.net",
    "relay.xero.systems",
    "relay.feeds.blue",
    "zlay.waow.tech",
    "asia.firehose.network",
    "europe.firehose.network",
    "northamerica.firehose.network",
    "relay.bas.sh",
    "relay.t4tlabs.net",
    "relay.waow.tech",
];

fn is_known_relay(url_bytes: &[u8]) -> bool {
    let Ok(url_str) = std::str::from_utf8(url_bytes) else {
        return false;
    };
    let Ok(url) = url::Url::parse(url_str) else {
        return false;
    };
    url.host_str()
        .is_some_and(|h| KNOWN_RELAY_HOSTS.contains(&h))
}

pub(super) fn firehose_source_is_pds(db: &Db, batch: &mut OwnedWriteBatch) -> Result<()> {
    let relay_bytes = rmp_serde::to_vec(&FirehoseSourceMeta { is_pds: false })
        .map_err(|e| miette::miette!("failed to serialize meta: {e}"))?;
    let pds_bytes = rmp_serde::to_vec(&FirehoseSourceMeta { is_pds: true })
        .map_err(|e| miette::miette!("failed to serialize meta: {e}"))?;

    for item in db.crawler.prefix(keys::FIREHOSE_SOURCE_PREFIX) {
        let (key, val) = item.into_inner().into_diagnostic()?;
        if !val.is_empty() {
            continue;
        }
        let url_bytes = &key[keys::FIREHOSE_SOURCE_PREFIX.len()..];
        let value = if is_known_relay(url_bytes) {
            &relay_bytes
        } else {
            &pds_bytes
        };
        batch.insert(&db.crawler, key, value);
    }

    Ok(())
}
