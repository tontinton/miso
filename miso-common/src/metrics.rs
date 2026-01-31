use once_cell::sync::Lazy;
use prometheus::{
    Histogram, HistogramVec, IntCounterVec, IntGauge, IntGaugeVec, exponential_buckets,
    register_histogram, register_histogram_vec, register_int_counter_vec, register_int_gauge,
    register_int_gauge_vec,
};

pub static METRICS: Lazy<Metrics> = Lazy::new(Metrics::default);

// Connector name labels
pub const CONNECTOR_ELASTICSEARCH: &str = "elasticsearch";
pub const CONNECTOR_QUICKWIT: &str = "quickwit";
pub const CONNECTOR_SPLUNK: &str = "splunk";

// Connector operation labels
pub const OP_BEGIN_SEARCH: &str = "begin_search";
pub const OP_CONTINUE_SCROLL: &str = "continue_scroll";
pub const OP_GET_INDEXES: &str = "get_indexes";
pub const OP_SEARCH_AGGREGATION: &str = "search_aggregation";
pub const OP_CREATE_JOB: &str = "create_job";
pub const OP_FETCH_RESULTS: &str = "fetch_results";
pub const OP_POLL_JOB: &str = "poll_job";

// Status labels
pub const STATUS_SUCCESS: &str = "success";
pub const ERROR_UNKNOWN: &str = "unknown_error";
pub const ERROR_SERVER: &str = "server_error";
pub const ERROR_HTTP: &str = "http_error";
pub const ERROR_CONNECTOR: &str = "connector_error";
pub const ERROR_INTERNAL: &str = "internal_error";
pub const ERROR_EVAL: &str = "eval_error";

// Workflow step type labels
pub const STEP_SCAN: &str = "scan";
pub const STEP_FILTER: &str = "filter";
pub const STEP_SORT: &str = "sort";
pub const STEP_JOIN: &str = "join";
pub const STEP_SUMMARIZE: &str = "summarize";
pub const STEP_PROJECT: &str = "project";
pub const STEP_EXPAND: &str = "expand";
pub const STEP_LIMIT: &str = "limit";
pub const STEP_TOPN: &str = "topn";
pub const STEP_RENAME: &str = "rename";
pub const STEP_COUNT: &str = "count";
pub const STEP_TEE: &str = "tee";
pub const STEP_WRITE: &str = "write";

pub struct Metrics {
    pub query_latency: Histogram,
    pub running_queries: IntGauge,
    pub downloaded_bytes: IntCounterVec,
    pub alive_threads: IntGaugeVec,
    pub tokio_worker_threads: IntGauge,
    pub tokio_alive_tasks: IntGauge,
    pub connector_request_duration: HistogramVec,
    pub connector_requests_total: IntCounterVec,
    pub connector_errors_total: IntCounterVec,
    pub workflow_step_rows: IntCounterVec,
    pub workflow_step_errors: IntCounterVec,
    pub query_errors_total: IntCounterVec,
}

/// From 0.05s to 508.798s
fn duration_buckets() -> Vec<f64> {
    exponential_buckets(0.05, 1.85, 15).unwrap()
}

impl Default for Metrics {
    fn default() -> Self {
        Self {
            query_latency: register_histogram!(
                "query_latency",
                "Duration of /query route",
                duration_buckets()
            )
            .expect("create query_latency"),

            running_queries: register_int_gauge!(
                "running_queries",
                "Number of live running queries"
            )
            .expect("create running_queries"),

            downloaded_bytes: register_int_counter_vec!(
                "downloaded_bytes",
                "Number of bytes downloaded from a remote connector",
                &["connector"],
            )
            .expect("create downloaded_bytes"),

            alive_threads: register_int_gauge_vec!(
                "alive_threads",
                "Number of threads that are currently running",
                &["type"],
            )
            .expect("create alive_threads"),

            tokio_worker_threads: register_int_gauge!(
                "tokio_worker_threads",
                "Number of worker threads used by the tokio runtime",
            )
            .expect("create tokio_worker_threads"),

            tokio_alive_tasks: register_int_gauge!(
                "tokio_alive_tasks",
                "Number of alive tasks in the tokio runtime",
            )
            .expect("create tokio_alive_tasks"),

            connector_request_duration: register_histogram_vec!(
                "miso_connector_request_duration",
                "Duration of connector requests in seconds",
                &["connector", "operation"],
                duration_buckets(),
            )
            .expect("create connector_request_duration"),

            connector_requests_total: register_int_counter_vec!(
                "miso_connector_requests_total",
                "Total number of connector requests",
                &["connector", "status"]
            )
            .expect("create connector_requests_total"),

            connector_errors_total: register_int_counter_vec!(
                "miso_connector_errors_total",
                "Total number of connector errors",
                &["connector", "error_type"]
            )
            .expect("create connector_errors_total"),

            workflow_step_rows: register_int_counter_vec!(
                "miso_workflow_step_rows",
                "Number of rows processed by workflow steps",
                &["step_type"]
            )
            .expect("create workflow_step_rows"),

            workflow_step_errors: register_int_counter_vec!(
                "miso_workflow_step_errors",
                "Total number of errors in workflow steps",
                &["step_type", "error_type"]
            )
            .expect("create workflow_step_errors"),

            query_errors_total: register_int_counter_vec!(
                "miso_query_errors_total",
                "Total number of query errors",
                &["error_type"]
            )
            .expect("create query_errors_total"),
        }
    }
}
