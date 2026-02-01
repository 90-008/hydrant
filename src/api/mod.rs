use crate::state::AppState;
use axum::{routing::get, Router};
use std::{net::SocketAddr, sync::Arc};
use tower_http::cors::CorsLayer;
use tower_http::trace::TraceLayer;

mod debug;
pub mod repo;
pub mod stats;
mod stream;
pub mod xrpc;

pub async fn serve(state: Arc<AppState>, port: u16) -> miette::Result<()> {
    let app = Router::new()
        .route("/health", get(|| async { "OK" }))
        .route("/stats", get(stats::get_stats))
        .route("/stream", get(stream::handle_stream))
        .route("/debug/count", get(debug::handle_debug_count))
        .merge(xrpc::router())
        .merge(repo::router())
        .with_state(state)
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
