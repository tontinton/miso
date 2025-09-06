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

    #[clap(long, help = "Output logs formatted as JSON.", default_value = "false")]
    pub log_json: bool,

    #[clap(
        long,
        help = "Disable all optimizations (helpful for debugging purposes).",
        default_value = "false"
    )]
    pub no_optimizations: bool,

    #[clap(
        long,
        help = "Max distinct values of a field to be considered for dynamic filtering.",
        default_value = "10000"
    )]
    pub dynamic_filter_max_distinct_values: u32,
}

#[must_use]
pub fn parse_args() -> Args {
    Args::parse()
}
