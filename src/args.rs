use clap::{command, Parser};

#[derive(Parser, Debug, Clone)]
#[command(author, version, about, long_about = None)]
pub struct Args {
    #[clap(
        short,
        long,
        help = "Listen address (host:port).",
        default_value = "0.0.0.0:8080"
    )]
    pub listen: String,
}

#[must_use]
pub fn parse_args() -> Args {
    Args::parse()
}
