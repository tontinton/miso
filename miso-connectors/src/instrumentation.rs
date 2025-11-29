use crate::ConnectorError;
use color_eyre::eyre::Result;
use miso_common::metrics::{ERROR_HTTP, ERROR_SERVER, ERROR_UNKNOWN, METRICS, STATUS_SUCCESS};
use std::time::Instant;

pub fn error_type_label(err: &color_eyre::Report) -> &'static str {
    match err.downcast_ref::<ConnectorError>() {
        Some(ConnectorError::ServerResp(_, _)) => ERROR_SERVER,
        Some(ConnectorError::Http(_)) => ERROR_HTTP,
        _ => ERROR_UNKNOWN,
    }
}

pub fn record_operation_result<T>(
    connector_name: &str,
    operation: &str,
    result: &Result<T>,
    duration: f64,
) {
    METRICS
        .connector_request_duration
        .with_label_values(&[connector_name, operation])
        .observe(duration);

    match result {
        Ok(_) => {
            METRICS
                .connector_requests_total
                .with_label_values(&[connector_name, STATUS_SUCCESS])
                .inc();
        }
        Err(e) => {
            let error_type = error_type_label(e);
            METRICS
                .connector_errors_total
                .with_label_values(&[connector_name, error_type])
                .inc();
        }
    }
}

pub async fn instrument_operation<F, Fut, T>(
    connector_name: &str,
    operation: &str,
    f: F,
) -> Result<T>
where
    F: FnOnce() -> Fut,
    Fut: std::future::Future<Output = Result<T>>,
{
    let start = Instant::now();
    METRICS
        .connector_requests_total
        .with_label_values(&[connector_name, operation])
        .inc();

    let result = f().await;
    let duration = start.elapsed().as_secs_f64();

    record_operation_result(connector_name, operation, &result, duration);
    result
}
