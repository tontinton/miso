use std::pin::Pin;

use color_eyre::{Report, Result};
use futures_util::Stream;

#[macro_export]
macro_rules! try_next {
    ($iter:expr) => {
        match $iter.next() {
            Some($crate::log::LogItem::UnionSomePipelineDone) => {
                return Some($crate::log::LogItem::UnionSomePipelineDone);
            }
            Some($crate::log::LogItem::Err(e)) => return Some($crate::log::LogItem::Err(e)),
            Some($crate::log::LogItem::Log(log)) => Some(log),
            Some($crate::log::LogItem::PartialStreamLog(..)) => None,
            Some($crate::log::LogItem::PartialStreamDone(..)) => None,
            None => None,
        }
    };
}

#[macro_export]
macro_rules! try_next_with_partial_passthrough {
    ($iter:expr) => {
        match $iter.next() {
            Some($crate::log::LogItem::UnionSomePipelineDone) => {
                return Some($crate::log::LogItem::UnionSomePipelineDone);
            }
            Some($crate::log::LogItem::Err(e)) => return Some($crate::log::LogItem::Err(e)),
            Some($crate::log::LogItem::PartialStreamLog(log, id)) => {
                return Some($crate::log::LogItem::PartialStreamLog(log, id));
            }
            Some($crate::log::LogItem::PartialStreamDone(id)) => {
                return Some($crate::log::LogItem::PartialStreamDone(id));
            }
            Some($crate::log::LogItem::Log(log)) => Some(log),
            None => None,
        }
    };
}

pub enum PartialStreamItem {
    Log(Log),
    PartialStreamLog(Log, usize),
    PartialStreamDone(usize),
}

#[macro_export]
macro_rules! try_next_with_partial_stream {
    ($iter:expr) => {
        match $iter.next() {
            Some($crate::log::LogItem::UnionSomePipelineDone) => {
                return Some($crate::log::LogItem::UnionSomePipelineDone);
            }
            Some($crate::log::LogItem::Err(e)) => return Some($crate::log::LogItem::Err(e)),
            Some($crate::log::LogItem::Log(log)) => Some($crate::log::PartialStreamItem::Log(log)),
            Some($crate::log::LogItem::PartialStreamLog(log, id)) => {
                Some($crate::log::PartialStreamItem::PartialStreamLog(log, id))
            }
            Some($crate::log::LogItem::PartialStreamDone(id)) => {
                Some($crate::log::PartialStreamItem::PartialStreamDone(id))
            }
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
