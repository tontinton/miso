use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};

use color_eyre::{Report, Result};
use futures_util::Stream;

use crate::value::{Map, Value};

pub const COUNT_FIELD_NAME: &str = "Count";

pub type Log = Map<String, Value>;
pub type LogIter = Box<dyn Iterator<Item = LogItem>>;
pub type LogStream = Pin<Box<dyn Stream<Item = Log> + Send>>;
pub type LogTryStream = Pin<Box<dyn Stream<Item = Result<Log>> + Send>>;
pub type LogItemTryStream = Pin<Box<dyn Stream<Item = Result<LogItem>> + Send>>;

pub type SourceId = usize;

static SOURCE_ID_COUNTER: AtomicUsize = AtomicUsize::new(0);

pub fn next_source_id() -> SourceId {
    SOURCE_ID_COUNTER.fetch_add(1, Ordering::Relaxed)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct PartialStreamKey {
    pub partial_stream_id: usize,
    pub source_id: SourceId,
}

#[derive(Debug)]
pub enum LogItem<E = Report> {
    Log(Log),
    Err(E),
    PartialStreamLog(Log, PartialStreamKey),
    PartialStreamDone(PartialStreamKey),
    SourceDone(SourceId),
}

impl LogItem {
    pub fn attach_partial_stream_id(self, key: PartialStreamKey) -> LogItem {
        match self {
            LogItem::Log(log) => LogItem::PartialStreamLog(log, key),
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
