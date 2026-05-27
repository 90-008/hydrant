use crate::control::{ApiBinds, Hydrant};
use crate::state::AppState;
use axum::{Router, routing::get};
use std::{net::SocketAddr, sync::Arc};
use tower_http::cors::CorsLayer;
use tower_http::trace::TraceLayer;

const LISTEN_BACKLOG: i32 = 1024;

#[cfg(feature = "indexer")]
mod crawler;
mod db;
mod debug;
mod filter;
mod firehose;
mod ingestion;
#[cfg(feature = "jetstream")]
mod jetstream;
mod pds;
mod repos;
mod stats;
#[cfg(feature = "indexer_stream")]
mod stream;
#[cfg(any(feature = "relay", feature = "indexer_stream"))]
mod ws;
mod xrpc;

pub async fn serve(hydrant: Hydrant, binds: ApiBinds) -> miette::Result<()> {
    let blocks_available = hydrant.state.is_block_storage_enabled();
    let app = Router::new()
        .route(
            "/",
            get(async || {
                let kind = cfg!(feature = "indexer")
                    .then_some("indexer")
                    .unwrap_or("relay");
                let subscribe = cfg!(feature = "indexer")
                    .then_some("/stream")
                    .unwrap_or("/xrpc/com.atproto.sync.subscribeRepos");
                include_str!("index.txt")
                    .replace("%type%", kind)
                    .replace("%subscribe_url%", subscribe)
            }),
        )
        .route("/health", get(async || "OK"))
        .route("/_health", get(async || "OK"))
        .route("/stats", get(stats::get_stats));
    #[cfg(feature = "indexer_stream")]
    let app = app.nest("/stream", stream::router());
    #[cfg(feature = "jetstream")]
    let app = app.route("/subscribe", get(jetstream::handle_subscribe));
    let app = app
        .merge(xrpc::router(blocks_available))
        .merge(filter::router())
        .merge(pds::router())
        .merge(repos::router())
        .merge(ingestion::router())
        .merge(firehose::router())
        .merge(db::router());

    #[cfg(feature = "indexer")]
    let app = app.merge(crawler::router());

    #[cfg(feature = "backlinks")]
    let app = app.merge(crate::backlinks::api::router());

    let app = app
        .with_state(hydrant)
        .layer(TraceLayer::new_for_http())
        .layer(CorsLayer::permissive());

    let bind_list: Vec<SocketAddr> = binds.iter().collect();
    let listeners: Vec<tokio::net::TcpListener> = bind_list
        .iter()
        .map(|&addr| {
            let v6only = v6only_for(addr, &bind_list);
            let listener = bind_listener(addr, v6only)
                .map_err(|e| miette::miette!("failed to bind to {addr}: {e}"))?;
            tracing::info!("API server listening on {}", listener.local_addr().unwrap());
            Ok::<_, miette::Report>(listener)
        })
        .collect::<Result<_, _>>()?;

    let services = listeners.into_iter().map(|listener| {
        let app = app.clone();
        async move {
            axum::serve(
                listener,
                app.into_make_service_with_connect_info::<SocketAddr>(),
            )
            .await
            .map_err(|e| miette::miette!("axum server error: {e}"))
        }
    });

    futures::future::try_join_all(services).await?;

    Ok(())
}

fn v6only_for(addr: SocketAddr, all: &[SocketAddr]) -> Option<bool> {
    let SocketAddr::V6(v6) = addr else {
        return None;
    };
    let has_v4_sibling = all
        .iter()
        .any(|a| matches!(a, SocketAddr::V4(v4) if v4.port() == v6.port()));
    Some(has_v4_sibling)
}

fn bind_listener(
    addr: SocketAddr,
    v6only: Option<bool>,
) -> std::io::Result<tokio::net::TcpListener> {
    let domain = match addr {
        SocketAddr::V4(_) => socket2::Domain::IPV4,
        SocketAddr::V6(_) => socket2::Domain::IPV6,
    };
    let socket = socket2::Socket::new(domain, socket2::Type::STREAM, Some(socket2::Protocol::TCP))?;
    if let Some(only) = v6only {
        socket.set_only_v6(only)?;
    }
    socket.set_reuse_address(true)?;
    socket.set_nonblocking(true)?;
    socket.bind(&addr.into())?;
    socket.listen(LISTEN_BACKLOG)?;
    let std_listener: std::net::TcpListener = socket.into();
    tokio::net::TcpListener::from_std(std_listener)
}

pub async fn serve_debug(state: Arc<AppState>, port: u16) -> miette::Result<()> {
    let app = debug::router()
        .with_state(state)
        .layer(TraceLayer::new_for_http());

    let listener = tokio::net::TcpListener::bind(format!("127.0.0.1:{port}"))
        .await
        .map_err(|e| miette::miette!("failed to bind debug server to port {port}: {e}"))?;

    tracing::info!(
        "debug server listening on {}",
        listener.local_addr().unwrap()
    );

    axum::serve(
        listener,
        app.into_make_service_with_connect_info::<SocketAddr>(),
    )
    .await
    .map_err(|e| miette::miette!("debug server error: {e}"))?;

    Ok(())
}
