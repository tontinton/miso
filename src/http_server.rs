use axum::{routing::get, Router};

async fn root() -> &'static str {
    "Hello, World!"
}

#[must_use]
pub fn create_axum_app() -> Router {
    Router::new().route("/", get(root))
}
