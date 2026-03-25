use futures::FutureExt;
use hydrant::config::Config;
use hydrant::control::Hydrant;
use mimalloc::MiMalloc;

struct AppConfig {
    api_port: u16,
    enable_debug: bool,
    debug_port: u16,
}

impl AppConfig {
    fn from_env() -> Self {
        use hydrant::__cfg as cfg;
        let api_port = cfg!("API_PORT", 3000u16);
        let enable_debug = cfg!("ENABLE_DEBUG", false);
        let debug_port: u16 = api_port + 1;
        let debug_port = cfg!("DEBUG_PORT", debug_port);
        Self {
            api_port,
            enable_debug,
            debug_port,
        }
    }
}

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

    let debug_fut = app
        .enable_debug
        .then(|| hydrant.serve_debug(app.debug_port).boxed())
        .unwrap_or_else(|| std::future::pending().boxed());

    tokio::select! {
        r = hydrant.run()? => r,
        r = hydrant.serve(app.api_port) => r,
        r = debug_fut => r,
    }
}
