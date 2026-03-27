use std::ops::Not;
use std::str::FromStr;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use jacquard_common::Data;
use jacquard_common::IntoStatic;
use jacquard_common::types::crypto::PublicKey;
use jacquard_common::types::ident::AtIdentifier;
use jacquard_common::types::string::Did;
use jacquard_common::types::string::Handle;
use jacquard_identity::JacquardResolver;
use jacquard_identity::resolver::{
    IdentityError, IdentityErrorKind, IdentityResolver, PlcSource, ResolverOptions,
};
use miette::{Diagnostic, IntoDiagnostic};
use reqwest::StatusCode;
use scc::HashCache;
use smol_str::SmolStr;
use thiserror::Error;
use url::Url;

use crate::util::url_to_fluent_uri;

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

#[derive(Clone)]
pub struct MiniDoc {
    pub pds: Url,
    pub handle: Option<Handle<'static>>,
    pub key: Option<PublicKey<'static>>,
}

struct ResolverInner {
    jacquards: Vec<JacquardResolver>,
    next_idx: AtomicUsize,
    cache: HashCache<Did<'static>, MiniDoc>,
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
            opts.plc_source = PlcSource::PlcDirectory {
                base: url_to_fluent_uri(&url),
            };
            opts.request_timeout = Some(Duration::from_secs(3));

            jacquards.push(JacquardResolver::new(http.clone(), opts).with_system_dns());
        }

        if jacquards.is_empty() {
            panic!("at least one PLC URL must be provided");
        }

        Self {
            inner: Arc::new(ResolverInner {
                jacquards,
                next_idx: AtomicUsize::new(0),
                cache: HashCache::with_capacity(
                    std::cmp::min(1000, (identity_cache_size / 100) as usize),
                    identity_cache_size as usize,
                ),
            }),
        }
    }

    pub async fn invalidate(&self, did: &Did<'_>) {
        self.inner
            .cache
            .remove_async(&did.clone().into_static())
            .await;
    }

    pub fn invalidate_sync(&self, did: &Did<'_>) {
        self.inner.cache.remove_sync(&did.clone().into_static());
    }

    async fn req<'r, T, Fut>(
        &'r self,
        is_plc: bool,
        f: impl Fn(&'r JacquardResolver) -> Fut,
    ) -> Result<T, ResolverError>
    where
        Fut: Future<Output = Result<T, IdentityError>>,
    {
        let mut idx =
            self.inner.next_idx.fetch_add(1, Ordering::Relaxed) % self.inner.jacquards.len();
        let mut try_count = 0;
        loop {
            let res = f(&self.inner.jacquards[idx]).await;
            try_count += 1;
            // retry these with the different plc resolvers
            if is_plc {
                let is_retriable = matches!(
                    res.as_ref().map_err(|e| e.kind()),
                    Err(IdentityErrorKind::HttpStatus(StatusCode::TOO_MANY_REQUESTS)
                        | IdentityErrorKind::Transport(_))
                );
                // check if retriable and we haven't gone through all the plc resolvers
                if is_retriable && try_count < self.inner.jacquards.len() {
                    idx = (idx + 1) % self.inner.jacquards.len();
                    continue;
                }
            }
            return res.map_err(Into::into);
        }
    }

    pub async fn resolve_did(
        &self,
        identifier: &AtIdentifier<'_>,
    ) -> Result<Did<'static>, ResolverError> {
        match identifier {
            AtIdentifier::Did(did) => Ok(did.clone().into_static()),
            AtIdentifier::Handle(handle) => {
                let did = self.req(false, |j| j.resolve_handle(handle)).await?;
                Ok(did.into_static())
            }
        }
    }

    pub async fn resolve_doc(&self, did: &Did<'_>) -> Result<MiniDoc, ResolverError> {
        let did_static = did.clone().into_static();
        if let Some(entry) = self.inner.cache.get_async(&did_static).await {
            return Ok(entry.get().clone());
        }

        let doc_resp = self
            .req(did.starts_with("did:plc:"), |j| j.resolve_did_doc(did))
            .await?;
        let doc = doc_resp.parse()?;

        let pds = doc
            .pds_endpoint()
            .ok_or_else(|| miette::miette!("no PDS service found in DID Doc for {did}"))?;

        let mut handles = doc.handles();
        let handle = handles
            .is_empty()
            .not()
            .then(|| handles.remove(0).into_static());
        let key = doc.atproto_public_key().ok().flatten();

        let mini = MiniDoc {
            pds: Url::from_str(pds.as_str()).expect("that url is valid"),
            handle,
            key,
        };
        let _ = self.inner.cache.put_async(did_static, mini.clone()).await;
        Ok(mini)
    }

    pub async fn resolve_identity_info(
        &self,
        did: &Did<'_>,
    ) -> Result<(Url, Option<Handle<'static>>), ResolverError> {
        let mini = self.resolve_doc(did).await?;
        Ok((mini.pds, mini.handle))
    }

    pub async fn resolve_signing_key(
        &self,
        did: &Did<'_>,
    ) -> Result<PublicKey<'static>, ResolverError> {
        let did = did.clone().into_static();
        let mini = self.resolve_doc(&did).await?;
        Ok(mini
            .key
            .ok_or_else(|| NoSigningKeyError(did))
            .into_diagnostic()?)
    }

    /// resolves the full DID document as raw [`Data`] without caching, and
    /// extracts the first `alsoKnownAs` handle if present.
    pub async fn resolve_raw_doc(
        &self,
        did: &Did<'_>,
    ) -> Result<(Data<'static>, Option<Handle<'static>>), ResolverError> {
        let doc_resp = self
            .req(did.starts_with("did:plc:"), |j| j.resolve_did_doc(did))
            .await?;

        let handle = {
            let parsed = doc_resp.parse();
            parsed.ok().and_then(|doc| {
                let mut handles = doc.handles();
                handles
                    .is_empty()
                    .not()
                    .then(|| handles.remove(0).into_static())
            })
        };

        let data: Data<'_> = serde_json::from_slice(&doc_resp.buffer)
            .map_err(|e| ResolverError::Generic(miette::miette!("failed to parse DID doc: {e}")))?;

        Ok((data.into_static(), handle))
    }

    /// returns `true` if the given handle bi-directionally resolves to `did`.
    pub async fn verify_handle(
        &self,
        did: &Did<'_>,
        handle: &Handle<'_>,
    ) -> Result<bool, ResolverError> {
        let id = AtIdentifier::Handle(handle.clone().into_static());
        let resolved_did = self.resolve_did(&id).await?;
        Ok(resolved_did.as_str() == did.as_str())
    }
}

#[derive(Debug, Diagnostic, Error)]
#[error("no atproto signing key in DID doc for {0}")]
pub struct NoSigningKeyError(Did<'static>);
