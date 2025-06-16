use once_cell::sync::Lazy;
use prometheus::{Histogram, HistogramOpts, IntCounter, IntGauge};

pub static METRICS: Lazy<Metrics> = Lazy::new(Metrics::default);

pub struct Metrics {
    pub query_latency: Histogram,
    pub join_thread_spawn_latency: Histogram,
    pub running_queries: IntGauge,
    pub downloaded_bytes: IntCounter,
    pub tokio_worker_threads: IntGauge,
    pub tokio_alive_tasks: IntGauge,
}

impl Default for Metrics {
    fn default() -> Self {
        let query_latency = Histogram::with_opts(HistogramOpts::new(
            "query_latency",
            "Duration of /query route",
        ))
        .expect("create query_latency");
        let join_thread_spawn_latency = Histogram::with_opts(HistogramOpts::new(
            "join_thread_spawn_latency",
            "Duration of time waiting between spawn and start of a join thread",
        ))
        .expect("create join_thread_spawn_latency");
        let running_queries = IntGauge::new("running_queries", "Number of live running queries")
            .expect("create running_queries");
        let downloaded_bytes = IntCounter::new(
            "downloaded_bytes",
            "Number of bytes downloded from a remote connector",
        )
        .expect("create downloaded_bytes");
        let tokio_worker_threads = IntGauge::new(
            "tokio_worker_threads",
            "Number of worker threads used by the tokio runtime",
        )
        .expect("create tokio_worker_threads");
        let tokio_alive_tasks = IntGauge::new(
            "tokio_alive_tasks",
            "Number of alive tasks in the tokio runtime",
        )
        .expect("create tokio_alive_tasks");

        prometheus::register(Box::new(query_latency.clone())).expect("failed to register");
        prometheus::register(Box::new(join_thread_spawn_latency.clone()))
            .expect("failed to register");
        prometheus::register(Box::new(running_queries.clone())).expect("failed to register");
        prometheus::register(Box::new(downloaded_bytes.clone())).expect("failed to register");
        prometheus::register(Box::new(tokio_worker_threads.clone())).expect("failed to register");
        prometheus::register(Box::new(tokio_alive_tasks.clone())).expect("failed to register");

        Self {
            query_latency,
            join_thread_spawn_latency,
            running_queries,
            downloaded_bytes,
            tokio_worker_threads,
            tokio_alive_tasks,
        }
    }
}
