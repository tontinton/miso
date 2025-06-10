use color_eyre::eyre::{Context, Result};
use mimalloc::MiMalloc;
use tokio::{net::TcpListener, signal};
use tracing::{debug, info};

use miso::{args::parse_args, http_server::create_axum_app};

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
    color_eyre::install()?;
    tracing_subscriber::fmt::init();

    let args = parse_args();
    debug!(?args, "Init");

    let app = create_axum_app(&args)?;
    let listener = TcpListener::bind(&args.listen).await?;

    info!("Listening on {}", args.listen);
    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal())
        .await
        .context("axum serve")?;

    Ok(())
}
