#[allow(dead_code, unused_imports)]
pub mod predicate_pushdown;
#[allow(dead_code)]
pub(crate) mod test_cases;
mod test_context_layer;

use once_cell::sync::OnceCell;
use test_context_layer::TestContextLayer;

static TEST_LAYER: OnceCell<TestContextLayer> = OnceCell::new();

/// Initialize test infrastructure with color_eyre and tracing.
/// Call this from a #[ctor] function in each test file.
pub fn init_test_tracing() {
    use tracing_subscriber::{filter::EnvFilter, layer::SubscriberExt, util::SubscriberInitExt};

    color_eyre::install().unwrap();

    let test_layer = TestContextLayer::new();
    let env_filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));

    tracing_subscriber::registry()
        .with(env_filter)
        .with(test_layer.clone())
        .init();

    TEST_LAYER.set(test_layer).expect("set test layer")
}
