use std::ops::Not;

use jacquard::types::string::Handle;
use jacquard::IntoStatic;
use jacquard_common::types::ident::AtIdentifier;
use jacquard_common::types::string::Did;
use jacquard_identity::resolver::{IdentityResolver, PlcSource, ResolverOptions};
use jacquard_identity::JacquardResolver;
use miette::{IntoDiagnostic, Result};
use url::Url;

pub struct Resolver {
    inner: JacquardResolver,
}

impl Resolver {
    pub fn new(plc_url: Url) -> Self {
        let http = reqwest::Client::new();
        let mut opts = ResolverOptions::default();
        opts.plc_source = PlcSource::PlcDirectory { base: plc_url };

        let inner = JacquardResolver::new(http, opts);

        Self { inner }
    }

    pub async fn resolve_did(&self, identifier: &AtIdentifier<'_>) -> Result<Did<'static>> {
        match identifier {
            AtIdentifier::Did(did) => Ok(did.clone().into_static()),
            AtIdentifier::Handle(handle) => {
                let did = self.inner.resolve_handle(handle).await.into_diagnostic()?;
                Ok(did.into_static())
            }
        }
    }

    pub async fn resolve_identity_info(&self, did: &Did<'_>) -> Result<(Url, Option<Handle<'_>>)> {
        let doc_resp = self.inner.resolve_did_doc(did).await.into_diagnostic()?;
        let doc = doc_resp.parse().into_diagnostic()?;

        let pds = doc
            .pds_endpoint()
            .ok_or_else(|| miette::miette!("no PDS service found in DID Doc for {did}"))?;

        let mut handles = doc.handles();
        let handle = handles.is_empty().not().then(|| handles.remove(0));

        Ok((pds, handle))
    }
}
