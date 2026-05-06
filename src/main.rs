use futures::FutureExt;
use hydrant::config::Config;
use hydrant::control::{ApiBinds, Hydrant};
use mimalloc::MiMalloc;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};

const DEFAULT_API_PORT: u16 = 3000;
const DEFAULT_DEBUG_PORT: u16 = DEFAULT_API_PORT + 1;

fn default_api_binds() -> ApiBinds {
    ApiBinds::try_from_iter([
        SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), DEFAULT_API_PORT),
        SocketAddr::new(IpAddr::V6(Ipv6Addr::UNSPECIFIED), DEFAULT_API_PORT),
    ])
    .expect("two-element literal is non-empty")
}

struct AppConfig {
    api_binds: ApiBinds,
    enable_debug: bool,
    debug_port: u16,
}

impl AppConfig {
    fn from_env() -> miette::Result<Self> {
        use hydrant::__cfg as cfg;
        let api_binds = parse_api_binds()?;
        let enable_debug = cfg!("ENABLE_DEBUG", false);
        let debug_port_default = api_binds
            .iter()
            .next()
            .expect("ApiBinds is non-empty by construction")
            .port()
            .checked_add(1)
            .unwrap_or(DEFAULT_DEBUG_PORT);
        let debug_port = cfg!("DEBUG_PORT", debug_port_default);
        Ok(Self {
            api_binds,
            enable_debug,
            debug_port,
        })
    }
}

fn parse_api_binds() -> miette::Result<ApiBinds> {
    let Ok(raw) = std::env::var("HYDRANT_API_BIND") else {
        return Ok(default_api_binds());
    };
    let parsed = raw
        .split(',')
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(|s| {
            s.parse::<SocketAddr>()
                .map_err(|e| miette::miette!("invalid HYDRANT_API_BIND entry `{s}`: {e}"))
        })
        .collect::<miette::Result<Vec<_>>>()?;
    ApiBinds::try_from_iter(parsed)
        .ok_or_else(|| miette::miette!("HYDRANT_API_BIND is set but contains no addresses"))
}

#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

#[tokio::main]
async fn main() -> miette::Result<()> {
    rustls::crypto::aws_lc_rs::default_provider()
        .install_default()
        .ok();

    let cfg = Config::from_env()?;
    let app = AppConfig::from_env()?;

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
        r = hydrant.serve(app.api_binds) => r,
        r = debug_fut => r,
    }
}
