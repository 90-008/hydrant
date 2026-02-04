use std::ops::Not;
use std::sync::Arc;
use std::time::Duration;

use jacquard::IntoStatic;
use jacquard::types::string::Handle;
use jacquard_common::types::crypto::PublicKey;
use jacquard_common::types::ident::AtIdentifier;
use jacquard_common::types::string::Did;
use jacquard_identity::JacquardResolver;
use jacquard_identity::resolver::{IdentityResolver, PlcSource, ResolverOptions};
use miette::{IntoDiagnostic, Result};
use scc::HashCache;
use url::Url;

struct ResolverInner {
    jacquard: JacquardResolver,
    key_cache: HashCache<Did<'static>, PublicKey<'static>>,
}

#[derive(Clone)]
pub struct Resolver {
    inner: Arc<ResolverInner>,
}

impl Resolver {
    pub fn new(plc_url: Url, identity_cache_size: u64) -> Self {
        let http = reqwest::Client::new();
        let mut opts = ResolverOptions::default();
        opts.plc_source = PlcSource::PlcDirectory { base: plc_url };
        opts.request_timeout = Some(Duration::from_secs(3));

        // no jacquard cache - we manage our own
        let jacquard = JacquardResolver::new(http, opts);

        Self {
            inner: Arc::new(ResolverInner {
                jacquard,
                key_cache: HashCache::with_capacity(
                    std::cmp::min(1000, (identity_cache_size / 100) as usize),
                    identity_cache_size as usize,
                ),
            }),
        }
    }

    pub async fn resolve_did(&self, identifier: &AtIdentifier<'_>) -> Result<Did<'static>> {
        match identifier {
            AtIdentifier::Did(did) => Ok(did.clone().into_static()),
            AtIdentifier::Handle(handle) => {
                let did = self
                    .inner
                    .jacquard
                    .resolve_handle(handle)
                    .await
                    .into_diagnostic()?;
                Ok(did.into_static())
            }
        }
    }

    pub async fn resolve_identity_info(&self, did: &Did<'_>) -> Result<(Url, Option<Handle<'_>>)> {
        let doc_resp = self
            .inner
            .jacquard
            .resolve_did_doc(did)
            .await
            .into_diagnostic()?;
        let doc = doc_resp.parse().into_diagnostic()?;

        let pds = doc
            .pds_endpoint()
            .ok_or_else(|| miette::miette!("no PDS service found in DID Doc for {did}"))?;

        let mut handles = doc.handles();
        let handle = handles.is_empty().not().then(|| handles.remove(0));

        Ok((pds, handle))
    }

    pub async fn resolve_signing_key(&self, did: &Did<'_>) -> Result<PublicKey<'static>> {
        let did_static = did.clone().into_static();

        if let Some(entry) = self.inner.key_cache.get_async(&did_static).await {
            return Ok(entry.get().clone());
        }

        let doc_resp = self
            .inner
            .jacquard
            .resolve_did_doc(did)
            .await
            .into_diagnostic()?;
        let doc = doc_resp.parse().into_diagnostic()?;

        let key = doc
            .atproto_public_key()
            .into_diagnostic()?
            .ok_or_else(|| miette::miette!("no atproto signing key in DID doc for {did}"))?;

        let _ = self
            .inner
            .key_cache
            .put_async(did_static, key.clone())
            .await;

        Ok(key)
    }
}
