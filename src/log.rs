use std::pin::Pin;

use color_eyre::Result;
use futures_core::Stream;

pub type Log = serde_json::Map<String, serde_json::Value>;
pub type LogStream = Pin<Box<dyn Stream<Item = Log> + Send>>;
pub type LogTryStream = Pin<Box<dyn Stream<Item = Result<Log>> + Send>>;
