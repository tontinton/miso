use std::{
    pin::Pin,
    sync::atomic::{self, AtomicU64},
};

use async_stream::stream;
use axum::async_trait;
use color_eyre::Result;
use futures_core::Stream;
use futures_util::StreamExt;
use serde_json::Value;

use crate::log::{Log, LogStream};

use super::partial_stream::PartialStreamExecutor;

const COUNT_LOG_FIELD_NAME: &str = "count";

type CountStream = Pin<Box<dyn Stream<Item = u64> + Send>>;

pub fn count_to_log(count: u64) -> Log {
    let mut log = Log::new();
    log.insert(COUNT_LOG_FIELD_NAME.into(), Value::from(count));
    log
}

pub async fn count_stream(mut input_stream: LogStream) -> u64 {
    let mut count = 0;
    while input_stream.next().await.is_some() {
        count += 1;
    }
    count
}

fn mux_input_to_count_stream(mut input_stream: LogStream) -> CountStream {
    Box::pin(stream! {
        while let Some(mut log) = input_stream.next().await {
            if let Some(value) = log.remove(COUNT_LOG_FIELD_NAME) {
                if let Some(c) = value.as_u64() {
                    yield c;
                }
            }
        }
    })
}

pub async fn mux_count_stream(input_stream: LogStream) -> u64 {
    let mut count = 0;
    let mut count_stream = mux_input_to_count_stream(input_stream);
    while let Some(c) = count_stream.next().await {
        count += c;
    }
    count
}

#[derive(Default)]
pub struct PartialMuxCountExecutor {
    count: AtomicU64,
}

#[async_trait]
impl PartialStreamExecutor for PartialMuxCountExecutor {
    type Output = u64;

    async fn execute(&self, input_stream: LogStream) -> Result<Self::Output> {
        let mut count_stream = mux_input_to_count_stream(input_stream);
        while let Some(c) = count_stream.next().await {
            self.count.fetch_add(c, atomic::Ordering::Relaxed);
        }
        Ok(self.get_partial())
    }

    fn get_partial(&self) -> Self::Output {
        self.count.load(atomic::Ordering::Relaxed)
    }
}
