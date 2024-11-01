use color_eyre::eyre::{bail, Context, Result};
use reqwest::Client;
use tracing::instrument;

#[instrument(name = "GET request")]
pub async fn get_text(url: &str) -> Result<String> {
    let client = Client::new();
    let response = client.get(url).send().await.context("http request")?;
    if !response.status().is_success() {
        bail!("GET {} failed with status: {}", url, response.status());
    }
    Ok(response.text().await.context("text from response")?)
}
