use std::env;
use std::sync::Arc;
use std::time::Duration;

use hydrant::resolver::Resolver;
use jacquard::IntoStatic; // Corrected from jacquard_identity::IntoStatic
use jacquard::api::com_atproto::sync::get_repo::GetRepo;
use jacquard::prelude::XrpcExt;
use jacquard::types::did::Did;
use jacquard_common::types::ident::AtIdentifier;
use jacquard_repo::MemoryBlockStore;
use jacquard_repo::mst::Mst;
use miette::{IntoDiagnostic, Result};
use tracing::{Level, info};
use tracing_subscriber::FmtSubscriber; // Restored
use url::Url; // Restored

#[tokio::main]
async fn main() -> Result<()> {
    // Setup logging
    let subscriber = FmtSubscriber::builder()
        .with_max_level(Level::INFO)
        .finish();
    tracing::subscriber::set_global_default(subscriber).expect("setting default subscriber failed");

    // Parse args
    let args: Vec<String> = env::args().collect();
    if args.len() != 2 {
        eprintln!("Usage: {} <handle|did>", args[0]);
        std::process::exit(1);
    }
    let identifier_str = &args[1];

    // Init resolver
    let plc_url = Url::parse("https://plc.directory").into_diagnostic()?;
    let resolver = Resolver::new(plc_url, 100);

    // Resolve identity
    info!("Resolving {}...", identifier_str);
    let identifier = if identifier_str.starts_with("did:") {
        AtIdentifier::Did(Did::new(identifier_str).map_err(|e| miette::miette!("{}", e))?)
    } else {
        AtIdentifier::Handle(identifier_str.parse().into_diagnostic()?)
    };

    let did = match identifier {
        AtIdentifier::Did(d) => d.into_static(),
        AtIdentifier::Handle(h) => {
            let d = resolver.resolve_did(&AtIdentifier::Handle(h)).await?;
            d
        }
    };
    info!("Resolved to DID: {}", did);

    let (pds_url, _) = resolver.resolve_identity_info(&did).await?;
    info!("PDS URL: {}", pds_url);

    // Fetch repo
    info!("Fetching repo...");
    let http = reqwest::Client::builder()
        .timeout(Duration::from_secs(30))
        .build()
        .into_diagnostic()?;

    let req = GetRepo::new().did(did.clone()).build();
    let resp = http.xrpc(pds_url).send(&req).await.into_diagnostic()?;
    let car_bytes = resp.into_output().map_err(|e| miette::miette!("{}", e))?; // explicit map_err

    info!("Fetched {} bytes", car_bytes.body.len());

    // Parse CAR
    let parsed = jacquard_repo::car::reader::parse_car_bytes(&car_bytes.body)
        .await
        .into_diagnostic()?;

    let store = Arc::new(MemoryBlockStore::new());
    for (_cid, bytes) in &parsed.blocks {
        jacquard_repo::BlockStore::put(store.as_ref(), bytes)
            .await
            .into_diagnostic()?;
    }

    // Load MST
    let root_bytes = parsed
        .blocks
        .get(&parsed.root)
        .ok_or_else(|| miette::miette!("root block missing from CAR"))?;

    let root_commit = jacquard_repo::commit::Commit::from_cbor(root_bytes).into_diagnostic()?;
    info!("Repo rev: {}", root_commit.rev);

    let mst: Mst<MemoryBlockStore> = Mst::load(store.clone(), root_commit.data, None);
    let leaves = mst.leaves().await.into_diagnostic()?;
    let root = mst.root().await.into_diagnostic()?;

    info!("Found {} records", leaves.len());

    println!("root -> {}", root);
    for (key, cid) in leaves {
        println!("{} -> {}", key, cid);
    }

    Ok(())
}
