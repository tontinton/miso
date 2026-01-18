use miso_workflow_types::log::{Log, PartialStreamKey, SourceId};

#[macro_export]
macro_rules! try_next {
    ($iter:expr) => {
        match $iter.next() {
            Some(miso_workflow_types::log::LogItem::SourceDone(id)) => {
                return Some(miso_workflow_types::log::LogItem::SourceDone(id));
            }
            Some(miso_workflow_types::log::LogItem::Err(e)) => {
                return Some(miso_workflow_types::log::LogItem::Err(e))
            }
            Some(miso_workflow_types::log::LogItem::Log(log)) => Some(log),
            Some(miso_workflow_types::log::LogItem::PartialStreamLog(..)) => None,
            Some(miso_workflow_types::log::LogItem::PartialStreamDone(..)) => None,
            None => None,
        }
    };
}

pub enum PartialStreamItem {
    Log(Log),
    PartialStreamLog(Log, PartialStreamKey),
    PartialStreamDone(PartialStreamKey),
    SourceDone(SourceId),
}

#[macro_export]
macro_rules! try_next_with_partial_stream {
    ($iter:expr) => {
        match $iter.next() {
            Some(miso_workflow_types::log::LogItem::SourceDone(id)) => {
                Some($crate::log_utils::PartialStreamItem::SourceDone(id))
            }
            Some(miso_workflow_types::log::LogItem::Err(e)) => {
                return Some(miso_workflow_types::log::LogItem::Err(e))
            }
            Some(miso_workflow_types::log::LogItem::Log(log)) => {
                Some($crate::log_utils::PartialStreamItem::Log(log))
            }
            Some(miso_workflow_types::log::LogItem::PartialStreamLog(log, key)) => Some(
                $crate::log_utils::PartialStreamItem::PartialStreamLog(log, key),
            ),
            Some(miso_workflow_types::log::LogItem::PartialStreamDone(key)) => {
                Some($crate::log_utils::PartialStreamItem::PartialStreamDone(key))
            }
            None => None,
        }
    };
}
