use miso_workflow_types::log::Log;

#[macro_export]
macro_rules! try_next {
    ($iter:expr) => {
        match $iter.next() {
            Some(miso_workflow_types::log::LogItem::UnionSomePipelineDone) => {
                return Some(miso_workflow_types::log::LogItem::UnionSomePipelineDone);
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
    PartialStreamLog(Log, usize),
    PartialStreamDone(usize),
}

#[macro_export]
macro_rules! try_next_with_partial_stream {
    ($iter:expr) => {
        match $iter.next() {
            Some(miso_workflow_types::log::LogItem::UnionSomePipelineDone) => {
                return Some(miso_workflow_types::log::LogItem::UnionSomePipelineDone);
            }
            Some(miso_workflow_types::log::LogItem::Err(e)) => {
                return Some(miso_workflow_types::log::LogItem::Err(e))
            }
            Some(miso_workflow_types::log::LogItem::Log(log)) => {
                Some($crate::log_utils::PartialStreamItem::Log(log))
            }
            Some(miso_workflow_types::log::LogItem::PartialStreamLog(log, id)) => Some(
                $crate::log_utils::PartialStreamItem::PartialStreamLog(log, id),
            ),
            Some(miso_workflow_types::log::LogItem::PartialStreamDone(id)) => {
                Some($crate::log_utils::PartialStreamItem::PartialStreamDone(id))
            }
            None => None,
        }
    };
}
