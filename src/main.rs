use color_eyre::eyre::Result;
use tracing::info;

fn main() -> Result<()> {
    tracing_subscriber::fmt::init();

    info!("Hello, world!");

    Ok(())
}
