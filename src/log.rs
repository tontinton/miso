use std::pin::Pin;

use color_eyre::{Report, Result};
use futures_util::Stream;

#[macro_export]
macro_rules! try_next {
    ($iter:expr) => {
        match $iter.next() {
            Some($crate::log::LogItem::OneRxDone) => return Some($crate::log::LogItem::OneRxDone),
            Some($crate::log::LogItem::Err(e)) => return Some($crate::log::LogItem::Err(e)),
            Some($crate::log::LogItem::Log(log)) => Some(log),
            None => None,
        }
    };
}

pub type Log = serde_json::Map<String, serde_json::Value>;
pub type LogIter = Box<dyn Iterator<Item = LogItem>>;
pub type LogStream = Pin<Box<dyn Stream<Item = Log> + Send>>;
pub type LogTryStream = Pin<Box<dyn Stream<Item = Result<Log>> + Send>>;

#[derive(Debug)]
pub enum LogItem<E = Report> {
    Log(Log),
    Err(E),
    OneRxDone,
}

impl LogItem {
    pub fn map_log(self, map_fn: impl Fn(Log) -> Log) -> LogItem {
        match self {
            LogItem::Log(log) => LogItem::Log(map_fn(log)),
            other => other,
        }
    }
}

impl From<Result<Log>> for LogItem {
    fn from(value: Result<Log>) -> Self {
        match value {
            Ok(log) => LogItem::Log(log),
            Err(e) => LogItem::Err(e),
        }
    }
}
