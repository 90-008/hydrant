use hydrant::config::{AppConfig, Config};
use hydrant::control::Hydrant;
use mimalloc::MiMalloc;

#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

#[tokio::main]
async fn main() -> miette::Result<()> {
    rustls::crypto::aws_lc_rs::default_provider()
        .install_default()
        .ok();

    let cfg = Config::from_env()?;
    let app = AppConfig::from_env();

    let env_filter = tracing_subscriber::EnvFilter::builder()
        .with_default_directive(tracing::Level::INFO.into())
        .from_env_lossy();
    tracing_subscriber::fmt().with_env_filter(env_filter).init();

    let hydrant = Hydrant::new(cfg).await?;

    if app.enable_debug {
        tokio::select! {
            r = hydrant.run() => r,
            r = hydrant.serve(app.api_port) => r,
            r = hydrant.serve_debug(app.debug_port) => r,
        }
    } else {
        tokio::select! {
            r = hydrant.run() => r,
            r = hydrant.serve(app.api_port) => r,
        }
    }
}
