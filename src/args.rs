use clap::Parser;

#[derive(Parser, Debug, Clone)]
#[command(author, version, about, long_about = None)]
pub struct Args {
    /// Path to JSON file with configurations (e.g. connectors to create on startup).
    #[clap(short, long)]
    pub config: Option<String>,

    /// Listen address (host:port).
    #[clap(short, long, default_value = "0.0.0.0:8080")]
    pub listen: String,

    /// Output logs formatted as JSON.
    #[clap(long, default_value = "false")]
    pub log_json: bool,

    /// Disable all optimizations (helpful for debugging purposes).
    #[clap(long, default_value = "false")]
    pub no_optimizations: bool,

    /// Max distinct values of a field to be considered for dynamic filtering.
    #[clap(long, default_value = "10000")]
    pub dynamic_filter_max_distinct_values: u64,

    /// OTLP endpoint for exporting traces (e.g., http://localhost:4317).
    #[clap(long)]
    pub otlp_endpoint: Option<String>,
}

#[must_use]
pub fn parse_args() -> Args {
    Args::parse()
}
