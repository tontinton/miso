use std::{
    collections::HashMap,
    fmt,
    sync::{Arc, Mutex},
};
use tracing::{span, Event, Subscriber};
use tracing_subscriber::{layer::Context, registry::LookupSpan, Layer};

pub const TEST_NAME_FIELD: &str = "test_name";

/// A tracing layer that captures logs per test by propagating test_name through span context.
///
/// This layer works similarly to QueryIdLayer - it propagates test_name from parent spans
/// to child spans through extensions, which works across tokio::spawn boundaries when
/// tasks are instrumented with `.instrument(Span::current())` / `in_current_span()`.
///
/// All log events are buffered in memory and can be retrieved with `drain_logs()` after
/// test completion for sequential output.
#[derive(Clone, Debug)]
pub struct TestContextLayer {
    buffers: Arc<Mutex<HashMap<String, Vec<String>>>>,
}

impl TestContextLayer {
    pub fn new() -> Self {
        Self {
            buffers: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// Drain (remove and return) all buffered logs for a specific test.
    /// Returns an empty Vec if no logs were captured for this test.
    pub fn drain_logs(&self, test_name: &str) -> Vec<String> {
        self.buffers
            .lock()
            .unwrap()
            .remove(test_name)
            .unwrap_or_default()
    }
}

impl Default for TestContextLayer {
    fn default() -> Self {
        Self::new()
    }
}

/// Storage for test_name value in span extensions.
#[derive(Clone, Debug)]
pub struct TestContextValue(pub String);

impl<S> Layer<S> for TestContextLayer
where
    S: Subscriber + for<'a> LookupSpan<'a>,
{
    fn on_new_span(&self, attrs: &span::Attributes<'_>, id: &span::Id, ctx: Context<'_, S>) {
        let span = ctx.span(id).expect("span must exist");

        let mut visitor = GetTestNameInCurrentSpan { test_name: None };
        attrs.record(&mut visitor);

        let test_name = visitor.test_name.or_else(|| {
            // Not found in current span, inherit from parent.
            span.parent().and_then(|parent| {
                parent
                    .extensions()
                    .get::<TestContextValue>()
                    .map(|v| v.0.clone())
            })
        });

        if let Some(test_name_value) = test_name {
            span.extensions_mut()
                .insert(TestContextValue(test_name_value));
        }
    }

    fn on_event(&self, event: &Event<'_>, ctx: Context<'_, S>) {
        let test_name = find_test_name(&ctx);

        if let Some(test_name) = test_name {
            let log_line = format_event(event, &ctx);
            self.buffers
                .lock()
                .unwrap()
                .entry(test_name)
                .or_default()
                .push(log_line);
        }
    }
}

struct GetTestNameInCurrentSpan {
    test_name: Option<String>,
}

impl tracing::field::Visit for GetTestNameInCurrentSpan {
    fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn fmt::Debug) {
        if field.name() == TEST_NAME_FIELD {
            self.test_name = Some(format!("{:?}", value).trim_matches('"').to_string());
        }
    }

    fn record_str(&mut self, field: &tracing::field::Field, value: &str) {
        if field.name() == TEST_NAME_FIELD {
            self.test_name = Some(value.to_string());
        }
    }
}

/// Find test_name by walking up the span tree.
fn find_test_name<S>(ctx: &Context<'_, S>) -> Option<String>
where
    S: Subscriber + for<'a> LookupSpan<'a>,
{
    ctx.lookup_current().and_then(|mut span| loop {
        if let Some(test_context) = span.extensions().get::<TestContextValue>() {
            return Some(test_context.0.clone());
        }
        span = span.parent()?;
    })
}

/// Format a tracing event as a human-readable log line.
fn format_event<S>(event: &Event<'_>, _ctx: &Context<'_, S>) -> String
where
    S: Subscriber + for<'a> LookupSpan<'a>,
{
    let meta = event.metadata();
    let mut visitor = LogFieldVisitor { fields: Vec::new() };
    event.record(&mut visitor);

    let timestamp = humantime::format_rfc3339(std::time::SystemTime::now());
    let level = meta.level();
    let target = meta.target();

    let message = visitor
        .fields
        .iter()
        .find(|(k, _)| k == "message")
        .map(|(_, v)| v.as_str())
        .unwrap_or("");

    let other_fields: Vec<String> = visitor
        .fields
        .iter()
        .filter(|(k, _)| k != "message")
        .map(|(k, v)| format!("{}={}", k, v))
        .collect();

    if other_fields.is_empty() {
        format!("[{}] {:5} {}: {}", timestamp, level, target, message)
    } else {
        format!(
            "[{}] {:5} {}: {} {}",
            timestamp,
            level,
            target,
            message,
            other_fields.join(" ")
        )
    }
}

struct LogFieldVisitor {
    fields: Vec<(String, String)>,
}

impl tracing::field::Visit for LogFieldVisitor {
    fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn fmt::Debug) {
        self.fields
            .push((field.name().to_string(), format!("{:?}", value)));
    }

    fn record_str(&mut self, field: &tracing::field::Field, value: &str) {
        self.fields
            .push((field.name().to_string(), value.to_string()));
    }

    fn record_i64(&mut self, field: &tracing::field::Field, value: i64) {
        self.fields
            .push((field.name().to_string(), value.to_string()));
    }

    fn record_u64(&mut self, field: &tracing::field::Field, value: u64) {
        self.fields
            .push((field.name().to_string(), value.to_string()));
    }

    fn record_bool(&mut self, field: &tracing::field::Field, value: bool) {
        self.fields
            .push((field.name().to_string(), value.to_string()));
    }

    fn record_f64(&mut self, field: &tracing::field::Field, value: f64) {
        self.fields
            .push((field.name().to_string(), value.to_string()));
    }
}
