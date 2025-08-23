use miso_workflow_types::{
    field::Field,
    log::{Log, LogItem, LogIter},
};

use crate::{
    interpreter::rename_field, log_utils::PartialStreamItem, try_next_with_partial_stream,
};

pub struct RenameIter {
    input: LogIter,
    renames: Vec<(Field, Field)>,
}

impl RenameIter {
    pub fn new(input: LogIter, renames: Vec<(Field, Field)>) -> Self {
        Self { input, renames }
    }

    fn rename(&self, mut log: Log) -> Log {
        for (from, to) in &self.renames {
            let _removed = rename_field(&mut log, from, to);
        }
        log
    }
}

impl Iterator for RenameIter {
    type Item = LogItem;

    fn next(&mut self) -> Option<Self::Item> {
        Some(match try_next_with_partial_stream!(self.input)? {
            PartialStreamItem::Log(log) => LogItem::Log(self.rename(log)),
            PartialStreamItem::PartialStreamLog(log, id) => {
                LogItem::PartialStreamLog(self.rename(log), id)
            }
            PartialStreamItem::PartialStreamDone(id) => LogItem::PartialStreamDone(id),
        })
    }
}
