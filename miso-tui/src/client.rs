use std::io::{BufRead, BufReader};
use std::sync::mpsc::Sender;

use color_eyre::Result;
use reqwest::blocking::Client;
use serde_json::json;

use crate::log::Log;

const SSE_DATA_PREFIX: &str = "data:";

pub fn query_stream(query: &str, tx: Sender<Log>) -> Result<()> {
    let client = Client::new();
    let resp = client
        .post("http://localhost:8080/query")
        .header("Accept", "text/event-stream")
        .json(&json!({"query": query}))
        .send()?
        .error_for_status()?;

    let mut reader = BufReader::new(resp);
    let mut buf = String::new();

    while reader.read_line(&mut buf)? > 0 {
        if buf.starts_with(SSE_DATA_PREFIX) {
            // SAFETY: `String` is uniquely owned here, so we can mutably access its bytes.
            let bytes = unsafe { buf.as_bytes_mut() };

            let mut slice = &mut bytes[SSE_DATA_PREFIX.len()..];

            let start = slice
                .iter()
                .position(|b| !b.is_ascii_whitespace())
                .unwrap_or(0);
            slice = &mut slice[start..];

            let end = slice
                .iter()
                .rposition(|b| !b.is_ascii_whitespace())
                .map(|i| i + 1)
                .unwrap_or(0);
            slice = &mut slice[..end];

            if let Ok(log) = Log::raw(slice) {
                let _ = tx.send(log);
            }
        }
        buf.clear();
    }

    Ok(())
}
