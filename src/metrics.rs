use once_cell::sync::Lazy;
use prometheus::{
    register_histogram, register_int_counter, register_int_gauge, register_int_gauge_vec,
    Histogram, IntCounter, IntGauge, IntGaugeVec,
};

pub static METRICS: Lazy<Metrics> = Lazy::new(Metrics::default);

pub struct Metrics {
    pub query_latency: Histogram,
    pub running_queries: IntGauge,
    pub downloaded_bytes: IntCounter,
    pub alive_threads: IntGaugeVec,
    pub tokio_worker_threads: IntGauge,
    pub tokio_alive_tasks: IntGauge,
}

impl Default for Metrics {
    fn default() -> Self {
        let query_latency = register_histogram!("query_latency", "Duration of /query route",)
            .expect("create query_latency");

        let running_queries =
            register_int_gauge!("running_queries", "Number of live running queries")
                .expect("create running_queries");

        let downloaded_bytes = register_int_counter!(
            "downloaded_bytes",
            "Number of bytes downloded from a remote connector",
        )
        .expect("create downloaded_bytes");

        let alive_threads = register_int_gauge_vec!(
            "alive_threads",
            "Number of threads that are currently running",
            &["type"],
        )
        .expect("create alive_threads");

        let tokio_worker_threads = register_int_gauge!(
            "tokio_worker_threads",
            "Number of worker threads used by the tokio runtime",
        )
        .expect("create tokio_worker_threads");

        let tokio_alive_tasks = register_int_gauge!(
            "tokio_alive_tasks",
            "Number of alive tasks in the tokio runtime",
        )
        .expect("create tokio_alive_tasks");

        Self {
            query_latency,
            running_queries,
            downloaded_bytes,
            alive_threads,
            tokio_worker_threads,
            tokio_alive_tasks,
        }
    }
}
