use crate::state::AppState;
use axum::extract::State;
use axum::routing::post;
use axum::{Router, routing::get};
use futures::FutureExt;
use miette::IntoDiagnostic;
use reqwest::StatusCode;
use std::{net::SocketAddr, sync::Arc};
use tower_http::cors::CorsLayer;
use tower_http::trace::TraceLayer;

mod debug;
mod filter;
mod repos;
mod stats;
mod stream;
mod xrpc;

pub async fn serve(state: Arc<AppState>, port: u16) -> miette::Result<()> {
    let app = Router::new()
        .route("/health", get(|| async { "OK" }))
        .route("/stats", get(stats::get_stats))
        .route("/_train_dict", post(handle_train_dict))
        .nest("/stream", stream::router())
        .merge(xrpc::router())
        .merge(filter::router())
        .merge(repos::router())
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

pub async fn handle_train_dict(
    State(state): State<Arc<AppState>>,
) -> Result<StatusCode, StatusCode> {
    let train = |name: &'static str| {
        let db = state.db.clone();
        tokio::task::spawn_blocking(move || db.train_dict(name))
            .map(|res| res.into_diagnostic().flatten())
    };
    let repos = train("repos");
    let blocks = train("blocks");
    let events = train("events");

    tokio::try_join!(repos, blocks, events).map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    Ok(StatusCode::OK)
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
