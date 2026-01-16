use color_eyre::eyre::{Context, Result};
use mimalloc::MiMalloc;
use miso_common::query_id_layer::{QueryIdJsonFormat, QueryIdLayer};
use miso_server::http_server::{create_app, OptimizationConfig};
use opentelemetry::{trace::TracerProvider as _, KeyValue};
use opentelemetry_otlp::WithExportConfig;
use opentelemetry_sdk::{
    trace::{Sampler, TracerProvider},
    Resource,
};
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

fn init_otlp_tracer(endpoint: &str) -> Result<TracerProvider> {
    let exporter = opentelemetry_otlp::SpanExporter::builder()
        .with_tonic()
        .with_endpoint(endpoint)
        .build()
        .context("failed to create OTLP exporter")?;

    let resource = Resource::new([KeyValue::new("service.name", "miso")]);

    let provider = TracerProvider::builder()
        .with_batch_exporter(exporter, opentelemetry_sdk::runtime::Tokio)
        .with_sampler(Sampler::AlwaysOn)
        .with_resource(resource)
        .build();

    Ok(provider)
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = parse_args();

    macro_rules! init_tracing {
        ($fmt_layer:expr, $tracer:expr) => {{
            let filter =
                EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
            let otlp_layer = $tracer.map(|t| tracing_opentelemetry::layer().with_tracer(t));
            tracing_subscriber::registry()
                .with(filter)
                .with(QueryIdLayer::new())
                .with($fmt_layer)
                .with(otlp_layer)
                .init();
        }};
    }

    let tracer = args
        .otlp_endpoint
        .as_ref()
        .map(|endpoint| init_otlp_tracer(endpoint))
        .transpose()?
        .map(|provider| provider.tracer("miso"));

    if args.log_json {
        init_tracing!(fmt::layer().event_format(QueryIdJsonFormat), tracer);
    } else {
        init_tracing!(fmt::layer(), tracer);
    }

    color_eyre::config::HookBuilder::default()
        .theme(color_eyre::config::Theme::new())
        .install()?;

    info!(?args, "Init");

    let config = if args.no_optimizations {
        OptimizationConfig::NoOptimizations
    } else {
        OptimizationConfig::WithOptimizations {
            dynamic_filter_max_distinct_values: args.dynamic_filter_max_distinct_values,
        }
    };

    let app = create_app(config, args.config.as_deref())?;
    let listener = TcpListener::bind(&args.listen).await?;

    info!("Listening on {}", args.listen);
    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal())
        .await
        .context("axum serve")?;

    Ok(())
}
