#[path = "../car.rs"]
mod car;
#[path = "../sparse_mst.rs"]
mod sparse_mst;

use std::collections::BTreeMap;
use std::io::Write;
use std::sync::Arc;
use std::time::{Duration, Instant};

use cid::Cid as IpldCid;
use futures::{StreamExt, stream};
use jacquard_api::com_atproto::sync::get_blocks::GetBlocks;
use jacquard_api::com_atproto::sync::get_record::GetRecord;
use jacquard_api::com_atproto::sync::get_repo::GetRepo;
use jacquard_common::deps::bytes::Bytes;
use jacquard_common::deps::fluent_uri;
use jacquard_common::types::cid::Cid as AtCid;
use jacquard_common::types::did::Did;
use jacquard_common::types::string::{Nsid, RecordKey};
use jacquard_common::xrpc::{XrpcError, XrpcExt};
use jacquard_repo::{BlockStore, MemoryBlockStore, Mst};
use miette::{IntoDiagnostic, Result, WrapErr};
use serde::Deserialize;
use smol_str::SmolStr;
use sparse_mst::{SparseScanner, mst_node_layer, sparse_probe_collection, sparse_ranges};
use url::Url;

const GET_BLOCKS_CHUNK: usize = 64;
const GET_BLOCKS_PARALLELISM: usize = 4;

#[derive(Debug)]
struct Args {
    collection: String,
    pattern: SmolStr,
    index_url: Url,
    plc_url: Url,
    limit: usize,
    probe_collection: Option<SmolStr>,
    probe_rkey: SmolStr,
}

impl Args {
    fn parse() -> Result<Self> {
        let mut collection = "sh.tangled.repo".to_string();
        let mut pattern = SmolStr::new("sh.tangled.*");
        let mut index_url = Url::parse("https://lightrail.microcosm.blue").into_diagnostic()?;
        let mut plc_url = Url::parse("https://plc.directory").into_diagnostic()?;
        let mut limit = 5usize;
        let mut probe_collection = None;
        let mut probe_rkey = SmolStr::new("-");

        let mut args = std::env::args().skip(1);
        while let Some(arg) = args.next() {
            let Some(value) = args.next() else {
                return Err(miette::miette!("missing value for {arg}"));
            };
            match arg.as_str() {
                "--collection" => collection = value,
                "--pattern" => pattern = SmolStr::new(value),
                "--index" => index_url = Url::parse(&value).into_diagnostic()?,
                "--plc" => plc_url = Url::parse(&value).into_diagnostic()?,
                "--limit" => limit = value.parse().into_diagnostic()?,
                "--probe-collection" => probe_collection = Some(SmolStr::new(value)),
                "--probe-rkey" => probe_rkey = SmolStr::new(value),
                _ => return Err(miette::miette!("unknown argument {arg}")),
            }
        }

        Ok(Self {
            collection,
            pattern,
            index_url,
            plc_url,
            limit,
            probe_collection,
            probe_rkey,
        })
    }
}

#[derive(Debug, Deserialize)]
struct ListReposByCollectionOutput {
    repos: Vec<RepoHit>,
}

#[derive(Debug, Deserialize)]
struct RepoHit {
    did: String,
}

#[derive(Debug, Deserialize)]
struct DidDoc {
    service: Vec<DidService>,
}

#[derive(Debug, Deserialize)]
struct DidService {
    #[serde(rename = "type")]
    kind: String,
    #[serde(rename = "serviceEndpoint")]
    service_endpoint: Url,
}

#[derive(Debug)]
struct FullBench {
    fetch: Duration,
    parse_and_walk: Duration,
    bytes: usize,
    blocks: usize,
    leaves: usize,
    matching: usize,
}

#[derive(Debug)]
struct SparseBench {
    total: Duration,
    requests: usize,
    auto_requests: usize,
    root_layer: Option<usize>,
    seed_bytes: usize,
    node_bytes: usize,
    record_bytes: usize,
    node_blocks: usize,
    records: usize,
}

#[derive(Debug, Default)]
struct BenchTotals {
    repos: usize,
    full_ms: u128,
    full_bytes: usize,
    sparse_ms: u128,
    sparse_bytes: usize,
}

impl BenchTotals {
    fn add(&mut self, full: &FullBench, sparse: &SparseBench) {
        self.repos += 1;
        self.full_ms += full.fetch.as_millis() + full.parse_and_walk.as_millis();
        self.full_bytes += full.bytes;
        self.sparse_ms += sparse.total.as_millis();
        self.sparse_bytes += sparse.seed_bytes + sparse.node_bytes + sparse.record_bytes;
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse()?;
    let http = reqwest::Client::builder()
        .timeout(Duration::from_secs(120))
        .zstd(true)
        .brotli(true)
        .gzip(true)
        .build()
        .into_diagnostic()?;

    let repos = list_repos(&http, &args).await?;
    let ranges = sparse_ranges(&[args.pattern.clone()]);

    println!(
        "did,pds,full_fetch_ms,full_parse_walk_ms,full_bytes,full_blocks,full_leaves,full_matching,sparse_total_ms,sparse_requests,auto_requests,sparse_root_layer,sparse_seed_bytes,sparse_node_bytes,sparse_record_bytes,sparse_node_blocks,sparse_records"
    );

    let mut totals = BenchTotals::default();
    for did in repos {
        let pds = match resolve_pds(&http, &args.plc_url, &did).await {
            Ok(pds) => pds,
            Err(err) => {
                eprintln!("skipping {did}: resolve failed: {err:?}");
                continue;
            }
        };
        let full = match bench_full(&http, &pds, &did, &ranges).await {
            Ok(full) => full,
            Err(err) => {
                eprintln!("skipping {did}: full bench failed: {err:?}");
                continue;
            }
        };
        let sparse = match bench_sparse(
            &http,
            &pds,
            &did,
            &[args.pattern.clone()],
            args.probe_collection.as_deref(),
            args.probe_rkey.as_str(),
        )
        .await
        {
            Ok(sparse) => sparse,
            Err(err) => {
                eprintln!("skipping {did}: sparse bench failed: {err:?}");
                continue;
            }
        };
        println!(
            "{did},{pds},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{}",
            full.fetch.as_millis(),
            full.parse_and_walk.as_millis(),
            full.bytes,
            full.blocks,
            full.leaves,
            full.matching,
            sparse.total.as_millis(),
            sparse.requests,
            sparse.auto_requests,
            sparse
                .root_layer
                .map(|layer| layer.to_string())
                .unwrap_or_default(),
            sparse.seed_bytes,
            sparse.node_bytes,
            sparse.record_bytes,
            sparse.node_blocks,
            sparse.records,
        );
        totals.add(&full, &sparse);
    }

    std::io::stdout().flush().into_diagnostic()?;
    eprintln!(
        "summary: repos={}, full_ms={}, full_bytes={}, sparse_ms={}, sparse_bytes={}",
        totals.repos, totals.full_ms, totals.full_bytes, totals.sparse_ms, totals.sparse_bytes
    );

    Ok(())
}

async fn list_repos(http: &reqwest::Client, args: &Args) -> Result<Vec<Did<'static>>> {
    let mut url = args
        .index_url
        .join("/xrpc/com.atproto.sync.listReposByCollection")
        .into_diagnostic()?;
    url.query_pairs_mut()
        .append_pair("collection", &args.collection)
        .append_pair("limit", &args.limit.to_string());

    let output = http
        .get(url)
        .send()
        .await
        .into_diagnostic()?
        .error_for_status()
        .into_diagnostic()?
        .json::<ListReposByCollectionOutput>()
        .await
        .into_diagnostic()?;

    output
        .repos
        .into_iter()
        .map(|repo| Did::new_owned(repo.did).into_diagnostic())
        .collect()
}

async fn resolve_pds(http: &reqwest::Client, plc: &Url, did: &Did<'static>) -> Result<Url> {
    let mut url = plc.clone();
    url.path_segments_mut()
        .map_err(|_| miette::miette!("plc url cannot be a base"))?
        .push(did.as_str());
    let doc = http
        .get(url)
        .send()
        .await
        .into_diagnostic()?
        .error_for_status()
        .into_diagnostic()?
        .json::<DidDoc>()
        .await
        .into_diagnostic()?;

    doc.service
        .into_iter()
        .find(|svc| svc.kind == "AtprotoPersonalDataServer")
        .map(|svc| svc.service_endpoint)
        .ok_or_else(|| miette::miette!("no pds service in did doc for {did}"))
}

async fn bench_full(
    http: &reqwest::Client,
    pds: &Url,
    did: &Did<'static>,
    ranges: &[sparse_mst::KeyRange],
) -> Result<FullBench> {
    let fetch_start = Instant::now();
    let req = GetRepo::new().did(did.clone()).build();
    let resp = http.xrpc(to_fluent_uri(pds)).send(&req).await?;
    let car = resp
        .into_output()
        .map_err(|err| miette::miette!("getRepo failed for {did}: {err}"))?;
    let fetch = fetch_start.elapsed();
    let bytes = car.body.len();

    let parse_start = Instant::now();
    let parsed = jacquard_repo::car::reader::parse_car_bytes(&car.body)
        .await
        .into_diagnostic()
        .wrap_err_with(|| format!("parse getRepo CAR for {did} ({} bytes)", car.body.len()))?;
    let blocks = parsed.blocks.len();
    let store = Arc::new(MemoryBlockStore::new_from_blocks(parsed.blocks));
    let root_bytes = store
        .get(&parsed.root)
        .await
        .into_diagnostic()?
        .ok_or_else(|| miette::miette!("root block missing from getRepo car"))?;
    let root_commit = jacquard_repo::commit::Commit::from_cbor(&root_bytes).into_diagnostic()?;
    let mst: Mst<MemoryBlockStore> = Mst::load(store, root_commit.data, None);
    let leaves = mst.leaves().await.into_diagnostic()?;
    let matching = leaves
        .iter()
        .filter(|(key, _)| ranges.iter().any(|range| range.contains(key)))
        .count();
    let parse_and_walk = parse_start.elapsed();

    Ok(FullBench {
        fetch,
        parse_and_walk,
        bytes,
        blocks,
        leaves: leaves.len(),
        matching,
    })
}

async fn bench_sparse(
    http: &reqwest::Client,
    pds: &Url,
    did: &Did<'static>,
    patterns: &[SmolStr],
    probe_collection: Option<&str>,
    probe_rkey: &str,
) -> Result<SparseBench> {
    let start = Instant::now();
    let ranges = sparse_ranges(patterns);
    let probe_collection = probe_collection
        .map(SmolStr::new)
        .or_else(|| sparse_probe_collection(patterns))
        .ok_or_else(|| miette::miette!("no sparse-compatible probe collection"))?;

    let req = GetRecord::new()
        .did(did.clone())
        .collection(Nsid::new_owned(probe_collection.as_str()).into_diagnostic()?)
        .rkey(RecordKey::any(probe_rkey).into_diagnostic()?)
        .build();
    let resp = http.xrpc(to_fluent_uri(pds)).send(&req).await?;
    let seed = resp
        .into_output()
        .map_err(|err| miette::miette!("getRecord seed failed for {did}: {err}"))?;
    let seed_bytes = seed.body.len();
    let parsed = jacquard_repo::car::reader::parse_car_bytes(&seed.body)
        .await
        .into_diagnostic()
        .wrap_err_with(|| {
            format!(
                "parse getRecord seed CAR for {did} ({} bytes)",
                seed.body.len()
            )
        })?;
    let root_bytes = parsed
        .blocks
        .get(&parsed.root)
        .ok_or_else(|| miette::miette!("root block missing from sparse seed car"))?;
    let root_commit = jacquard_repo::commit::Commit::from_cbor(root_bytes).into_diagnostic()?;
    let root_cid = root_commit.data;
    let root_layer = parsed
        .blocks
        .get(&root_cid)
        .map(|bytes| mst_node_layer(bytes))
        .transpose()?
        .flatten();

    let mut node_fetch_bytes = 0usize;
    let mut requests = 1usize;
    let mut scanner = SparseScanner::new(ranges, parsed.blocks);
    let scan = loop {
        match scanner.scan(root_cid)? {
            Ok(scan) => break scan,
            Err(missing) => {
                let (blocks, bytes, chunk_requests) =
                    fetch_blocks(http, pds, did, &missing).await?;
                node_fetch_bytes += bytes;
                requests += chunk_requests;
                scanner.insert_blocks(blocks);
            }
        }
    };

    let mut blocks = scanner.take_blocks();
    let missing_records: Vec<IpldCid> = scan
        .leaves
        .iter()
        .filter_map(|(_, cid)| (!blocks.contains_key(cid)).then_some(*cid))
        .collect();
    let (record_blocks, record_bytes, chunk_requests) =
        fetch_blocks(http, pds, did, &missing_records).await?;
    requests += chunk_requests;
    blocks.extend(record_blocks);

    let auto_requests = if requests > 1 && root_layer.is_some_and(|layer| layer <= 2) {
        2
    } else {
        requests
    };

    Ok(SparseBench {
        total: start.elapsed(),
        requests,
        auto_requests,
        root_layer,
        seed_bytes,
        node_bytes: scan.node_bytes_seen + node_fetch_bytes,
        record_bytes,
        node_blocks: scan.node_blocks_seen,
        records: scan.leaves.len(),
    })
}

async fn fetch_blocks(
    http: &reqwest::Client,
    pds: &Url,
    did: &Did<'static>,
    cids: &[IpldCid],
) -> Result<(BTreeMap<IpldCid, Bytes>, usize, usize)> {
    let mut out = BTreeMap::new();
    let mut bytes = 0usize;

    let fetches = cids
        .chunks(GET_BLOCKS_CHUNK)
        .filter(|chunk| !chunk.is_empty())
        .map(|chunk| fetch_block_chunk(http.clone(), pds.clone(), did.clone(), chunk.to_vec()))
        .collect::<Vec<_>>();
    let requests = fetches.len();
    let fetches = stream::iter(fetches).buffer_unordered(GET_BLOCKS_PARALLELISM);
    futures::pin_mut!(fetches);

    while let Some(chunk) = fetches.next().await {
        let (blocks, chunk_bytes) = chunk?;
        bytes += chunk_bytes;
        out.extend(blocks);
    }

    Ok((out, bytes, requests))
}

async fn fetch_block_chunk(
    http: reqwest::Client,
    pds: Url,
    did: Did<'static>,
    cids: Vec<IpldCid>,
) -> Result<(BTreeMap<IpldCid, Bytes>, usize)> {
    let req = GetBlocks::new()
        .did(did.clone())
        .cids(
            cids.iter()
                .map(|cid| AtCid::from(cid.to_string()))
                .collect::<Vec<_>>(),
        )
        .build();
    let resp = http.xrpc(to_fluent_uri(&pds)).send(&req).await?;
    let car = resp
        .into_output()
        .map_err(|err: XrpcError<_>| miette::miette!("getBlocks failed for {did}: {err}"))?;
    let bytes = car.body.len();
    let parsed = car::parse_car_blocks(car.body).wrap_err_with(|| {
        let cids = cids
            .iter()
            .map(ToString::to_string)
            .collect::<Vec<_>>()
            .join(",");
        format!(
            "parse getBlocks CAR for {did} ({} requested, {} bytes, cids={cids})",
            cids.len(),
            bytes
        )
    })?;
    Ok((parsed, bytes))
}

fn to_fluent_uri(url: &Url) -> fluent_uri::Uri<String> {
    fluent_uri::Uri::parse(url.as_str())
        .expect("validated url")
        .to_owned()
}
