use std::ops::Not;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use jacquard::IntoStatic;
use jacquard::types::string::Handle;
use jacquard_common::types::crypto::PublicKey;
use jacquard_common::types::ident::AtIdentifier;
use jacquard_common::types::string::Did;
use jacquard_identity::JacquardResolver;
use jacquard_identity::resolver::{
    IdentityError, IdentityErrorKind, IdentityResolver, PlcSource, ResolverOptions,
};
use miette::{Diagnostic, IntoDiagnostic};
use scc::HashCache;
use smol_str::SmolStr;
use thiserror::Error;
use url::Url;

#[derive(Debug, Diagnostic, Error)]
pub enum ResolverError {
    #[error("{0}")]
    Generic(miette::Report),
    #[error("too many requests")]
    Ratelimited,
    #[error("transport error: {0}")]
    Transport(SmolStr),
}

impl From<IdentityError> for ResolverError {
    fn from(e: IdentityError) -> Self {
        match e.kind() {
            IdentityErrorKind::HttpStatus(reqwest::StatusCode::TOO_MANY_REQUESTS) => {
                Self::Ratelimited
            }
            IdentityErrorKind::Transport(msg) => Self::Transport(msg.clone()),
            _ => Self::Generic(e.into()),
        }
    }
}

impl From<miette::Report> for ResolverError {
    fn from(report: miette::Report) -> Self {
        ResolverError::Generic(report)
    }
}

struct ResolverInner {
    jacquards: Vec<JacquardResolver>,
    next_idx: AtomicUsize,
    key_cache: HashCache<Did<'static>, PublicKey<'static>>,
}

#[derive(Clone)]
pub struct Resolver {
    inner: Arc<ResolverInner>,
}

impl Resolver {
    pub fn new(plc_urls: Vec<Url>, identity_cache_size: u64) -> Self {
        let http = reqwest::Client::new();
        let mut jacquards = Vec::with_capacity(plc_urls.len());

        for url in plc_urls {
            let mut opts = ResolverOptions::default();
            opts.plc_source = PlcSource::PlcDirectory { base: url };
            opts.request_timeout = Some(Duration::from_secs(3));

            // no jacquard cache - we manage our own
            jacquards.push(JacquardResolver::new(http.clone(), opts));
        }

        if jacquards.is_empty() {
            panic!("at least one PLC URL must be provided");
        }

        Self {
            inner: Arc::new(ResolverInner {
                jacquards,
                next_idx: AtomicUsize::new(0),
                key_cache: HashCache::with_capacity(
                    std::cmp::min(1000, (identity_cache_size / 100) as usize),
                    identity_cache_size as usize,
                ),
            }),
        }
    }

    fn get_jacquard(&self) -> &JacquardResolver {
        let idx = self.inner.next_idx.fetch_add(1, Ordering::Relaxed) % self.inner.jacquards.len();
        &self.inner.jacquards[idx]
    }

    pub async fn resolve_did(
        &self,
        identifier: &AtIdentifier<'_>,
    ) -> Result<Did<'static>, ResolverError> {
        match identifier {
            AtIdentifier::Did(did) => Ok(did.clone().into_static()),
            AtIdentifier::Handle(handle) => {
                let did = self.get_jacquard().resolve_handle(handle).await?;
                Ok(did.into_static())
            }
        }
    }

    pub async fn resolve_identity_info(
        &self,
        did: &Did<'_>,
    ) -> Result<(Url, Option<Handle<'_>>), ResolverError> {
        let doc_resp = self.get_jacquard().resolve_did_doc(did).await?;
        let doc = doc_resp.parse()?;

        let pds = doc
            .pds_endpoint()
            .ok_or_else(|| miette::miette!("no PDS service found in DID Doc for {did}"))?;

        let mut handles = doc.handles();
        let handle = handles.is_empty().not().then(|| handles.remove(0));

        Ok((pds, handle))
    }

    pub async fn resolve_signing_key(
        &self,
        did: &Did<'_>,
    ) -> Result<PublicKey<'static>, ResolverError> {
        let did = did.clone().into_static();
        if let Some(entry) = self.inner.key_cache.get_async(&did).await {
            return Ok(entry.get().clone());
        }

        let doc_resp = self.get_jacquard().resolve_did_doc(&did).await?;
        let doc = doc_resp.parse()?;

        let key = doc
            .atproto_public_key()
            .into_diagnostic()?
            .ok_or_else(|| NoSigningKeyError(did.clone()))
            .into_diagnostic()?;

        let _ = self.inner.key_cache.put_async(did, key.clone()).await;

        Ok(key)
    }
}

#[derive(Debug, Diagnostic, Error)]
#[error("no atproto signing key in DID doc for {0}")]
pub struct NoSigningKeyError(Did<'static>);
