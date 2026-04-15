use crate::control::Hydrant;
use crate::state::AppState;
use axum::{Router, routing::get};
use std::{net::SocketAddr, sync::Arc};
use tower_http::cors::CorsLayer;
use tower_http::trace::TraceLayer;

#[cfg(feature = "indexer")]
mod crawler;
mod db;
mod debug;
mod filter;
mod firehose;
mod ingestion;
mod pds;
mod repos;
mod stats;
#[cfg(feature = "indexer")]
mod stream;
mod xrpc;

pub async fn serve(hydrant: Hydrant, port: u16) -> miette::Result<()> {
    #[allow(unused_mut)]
    let mut app = Router::new()
        .route("/", get(async || include_str!("index.txt")))
        .route("/health", get(async || "OK"))
        .route("/_health", get(async || "OK"))
        .route("/stats", get(stats::get_stats));
    #[cfg(feature = "indexer")]
    let app = app.nest("/stream", stream::router());
    #[allow(unused_mut)]
    let mut app = app
        .merge(xrpc::router())
        .merge(filter::router())
        .merge(pds::router())
        .merge(repos::router())
        .merge(ingestion::router())
        .merge(firehose::router())
        .merge(db::router());

    #[cfg(feature = "indexer")]
    {
        app = app.merge(crawler::router());
    }

    #[cfg(feature = "backlinks")]
    let app = app.merge(crate::backlinks::api::router());

    let app = app
        .with_state(hydrant)
        .layer(TraceLayer::new_for_http())
        .layer(CorsLayer::permissive());

    let listener = tokio::net::TcpListener::bind(format!("0.0.0.0:{port}"))
        .await
        .map_err(|e| miette::miette!("failed to bind to port {port}: {e}"))?;

    tracing::info!("API server listening on {}", listener.local_addr().unwrap());

    axum::serve(
        listener,
        app.into_make_service_with_connect_info::<SocketAddr>(),
    )
    .await
    .map_err(|e| miette::miette!("axum server error: {e}"))?;

    Ok(())
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
