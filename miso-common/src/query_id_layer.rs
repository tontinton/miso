use serde_json::{Map, Value};
use std::{fmt, time::SystemTime};
use tracing::{Subscriber, span};
use tracing_subscriber::{
    Layer,
    fmt::{FmtContext, FormatEvent, FormatFields, format::Writer},
    layer::Context,
    registry::LookupSpan,
};

pub const QUERY_ID_FIELD: &str = "query_id";

/// A tracing layer that automatically propagates `query_id` from parent spans to child spans.
pub struct QueryIdLayer;

impl QueryIdLayer {
    pub fn new() -> Self {
        Self
    }
}

impl Default for QueryIdLayer {
    fn default() -> Self {
        Self::new()
    }
}

/// Storage for query_id value in span extensions.
#[derive(Clone, Debug)]
pub struct QueryIdValue(pub String);

impl<S> Layer<S> for QueryIdLayer
where
    S: Subscriber + for<'a> LookupSpan<'a>,
{
    fn on_new_span(&self, attrs: &span::Attributes<'_>, id: &span::Id, ctx: Context<'_, S>) {
        let span = ctx.span(id).expect("span must exist");

        let mut visitor = GetQueryIdInCurrentSpan { query_id: None };
        attrs.record(&mut visitor);

        let query_id = visitor.query_id.or_else(|| {
            // Not found in current span, inherit from parent.
            span.parent().and_then(|parent| {
                parent
                    .extensions()
                    .get::<QueryIdValue>()
                    .map(|v| v.0.clone())
            })
        });

        if let Some(query_id_value) = query_id {
            span.extensions_mut().replace(QueryIdValue(query_id_value));
        }
    }
}

struct GetQueryIdInCurrentSpan {
    query_id: Option<String>,
}

impl tracing::field::Visit for GetQueryIdInCurrentSpan {
    fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn fmt::Debug) {
        if field.name() == QUERY_ID_FIELD {
            self.query_id = Some(format!("{:?}", value));
        }
    }
}

/// Custom JSON formatter that injects query_id from span context as a top-level field.
pub struct QueryIdJsonFormat;

impl<S, N> FormatEvent<S, N> for QueryIdJsonFormat
where
    S: Subscriber + for<'a> LookupSpan<'a>,
    N: for<'a> FormatFields<'a> + 'static,
{
    fn format_event(
        &self,
        ctx: &FmtContext<'_, S, N>,
        mut writer: Writer<'_>,
        event: &tracing::Event<'_>,
    ) -> fmt::Result {
        let meta = event.metadata();
        let mut fields = Map::new();

        fields.insert(
            "timestamp".to_string(),
            Value::String(humantime::format_rfc3339(SystemTime::now()).to_string()),
        );

        fields.insert("level".to_string(), Value::String(meta.level().to_string()));

        if let Some(query_id) = find_query_id(ctx) {
            fields.insert("query_id".to_string(), Value::String(query_id));
        }

        let mut visitor = JsonFieldVisitor {
            fields: &mut fields,
        };
        event.record(&mut visitor);

        fields.insert(
            "target".to_string(),
            Value::String(meta.target().to_string()),
        );

        let json = serde_json::to_string(&fields).map_err(|_| fmt::Error)?;
        writeln!(writer, "{}", json)?;

        Ok(())
    }
}

/// Find query_id by walking up the span tree.
fn find_query_id<S, N>(ctx: &FmtContext<'_, S, N>) -> Option<String>
where
    S: Subscriber + for<'a> LookupSpan<'a>,
    N: for<'a> FormatFields<'a> + 'static,
{
    ctx.lookup_current().and_then(|mut span| {
        loop {
            if let Some(query_id) = span.extensions().get::<QueryIdValue>() {
                return Some(query_id.0.clone());
            }
            span = span.parent()?;
        }
    })
}

struct JsonFieldVisitor<'a> {
    fields: &'a mut Map<String, Value>,
}

impl<'a> tracing::field::Visit for JsonFieldVisitor<'a> {
    fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn fmt::Debug) {
        self.fields.insert(
            field.name().to_string(),
            Value::String(format!("{:?}", value)),
        );
    }

    fn record_str(&mut self, field: &tracing::field::Field, value: &str) {
        self.fields
            .insert(field.name().to_string(), Value::String(value.to_string()));
    }

    fn record_i64(&mut self, field: &tracing::field::Field, value: i64) {
        self.fields
            .insert(field.name().to_string(), Value::Number(value.into()));
    }

    fn record_u64(&mut self, field: &tracing::field::Field, value: u64) {
        self.fields
            .insert(field.name().to_string(), Value::Number(value.into()));
    }

    fn record_bool(&mut self, field: &tracing::field::Field, value: bool) {
        self.fields
            .insert(field.name().to_string(), Value::Bool(value));
    }
}
