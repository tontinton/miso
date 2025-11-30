use color_eyre::eyre::{Context, Result};
use mimalloc::MiMalloc;
use miso_common::query_id_layer::{QueryIdJsonFormat, QueryIdLayer};
use miso_server::http_server::{create_axum_app, OptimizationConfig};
use tokio::{net::TcpListener, signal};
use tracing::info;

use miso::args::parse_args;
use tracing_subscriber::{fmt, layer::SubscriberExt, util::SubscriberInitExt, EnvFilter};

#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

async fn shutdown_signal() {
    let ctrl_c = async {
        signal::ctrl_c()
            .await
            .expect("failed to install Ctrl+C handler");
        info!("SIGINT/Ctrl+C received, starting graceful shutdown");
    };

    #[cfg(unix)]
    let terminate = async {
        use tokio::signal::unix::{signal, SignalKind};

        // Kubernetes, Docker, systemd, etc...
        let mut sigterm =
            signal(SignalKind::terminate()).expect("failed to install SIGTERM handler");

        sigterm.recv().await;
        info!("SIGTERM received, starting graceful shutdown");
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => {},
        _ = terminate => {},
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = parse_args();

    if args.log_json {
        let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
        tracing_subscriber::registry()
            .with(filter)
            .with(QueryIdLayer::new())
            .with(fmt::layer().event_format(QueryIdJsonFormat))
            .init();

        color_eyre::config::HookBuilder::default()
            .theme(color_eyre::config::Theme::new())
            .install()?;
    } else {
        tracing_subscriber::fmt().init();
        color_eyre::install()?;
    }

    info!(?args, "Init");

    let config = if args.no_optimizations {
        OptimizationConfig::NoOptimizations
    } else {
        OptimizationConfig::WithOptimizations {
            dynamic_filter_max_distinct_values: args.dynamic_filter_max_distinct_values,
        }
    };

    let app = create_axum_app(config)?;
    let listener = TcpListener::bind(&args.listen).await?;

    info!("Listening on {}", args.listen);
    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal())
        .await
        .context("axum serve")?;

    Ok(())
}
