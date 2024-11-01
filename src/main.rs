use color_eyre::eyre::Result;
use tokio::net::TcpListener;
use tracing::{debug, info};

use miso::{args::parse_args, http_server::create_axum_app};

#[tokio::main]
async fn main() -> Result<()> {
    color_eyre::install()?;
    tracing_subscriber::fmt::init();

    let args = parse_args();

    debug!(?args, "Init");

    let listener = TcpListener::bind(&args.listen).await?;
    let app = create_axum_app()?;

    info!("Listening on {}", args.listen);
    axum::serve(listener, app).await?;

    Ok(())
}
