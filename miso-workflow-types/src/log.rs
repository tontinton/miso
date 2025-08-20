use std::pin::Pin;

use color_eyre::{Report, Result};
use futures_util::Stream;

use crate::value::{Map, Value};

pub type Log = Map<String, Value>;
pub type LogIter = Box<dyn Iterator<Item = LogItem>>;
pub type LogStream = Pin<Box<dyn Stream<Item = Log> + Send>>;
pub type LogTryStream = Pin<Box<dyn Stream<Item = Result<Log>> + Send>>;

#[derive(Debug)]
pub enum LogItem<E = Report> {
    Log(Log),
    Err(E),
    PartialStreamLog(Log, usize),
    PartialStreamDone(usize),
    UnionSomePipelineDone,
}

impl LogItem {
    pub fn attach_partial_stream_id(self, id: usize) -> LogItem {
        match self {
            LogItem::Log(log) => LogItem::PartialStreamLog(log, id),
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
