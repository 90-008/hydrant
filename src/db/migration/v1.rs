use std::str::FromStr;

use fjall::OwnedWriteBatch;
use miette::{Context, IntoDiagnostic, Result};
use url::Url;

use crate::db::{Db, keys};

/// migrates firehose cursors from `firehose_cursor|{url}` to `firehose_cursor|{host}`.
pub(super) fn stable_firehose_cursors(db: &Db, batch: &mut OwnedWriteBatch) -> Result<()> {
    let entries: Vec<(Vec<u8>, Vec<u8>)> = db
        .cursors
        .prefix(keys::FIREHOSE_CURSOR_PREFIX)
        .map(|item| {
            let (k, v) = item.into_inner().into_diagnostic()?;
            Ok((k.to_vec(), v.to_vec()))
        })
        .collect::<Result<_>>()?;

    for (old_key, value) in entries {
        let suffix = &old_key[keys::FIREHOSE_CURSOR_PREFIX.len()..];
        // old-format: suffix is a full URL containing "://" (e.g. "wss://bsky.network")
        // new-format (v1): suffix is just a hostname, no "://"
        if !suffix.windows(3).any(|w| w == b"://") {
            continue; // already in new format
        }
        let url_str = std::str::from_utf8(suffix)
            .into_diagnostic()
            .wrap_err("firehose cursor key contains non-utf8 url")?;
        let url = Url::from_str(url_str)
            .into_diagnostic()
            .wrap_err_with(|| format!("firehose cursor key contains invalid url {url_str:?}"))?;

        let new_key = keys::v1::firehose_cursor_key_from_url(&url);
        batch.insert(&db.cursors, &new_key, &value);
        batch.remove(&db.cursors, &old_key);
    }

    Ok(())
}
