use miso_common::metrics::{METRICS, STEP_RENAME};
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
    rows_processed: u64,
}

impl RenameIter {
    pub fn new(input: LogIter, renames: Vec<(Field, Field)>) -> Self {
        Self {
            input,
            renames,
            rows_processed: 0,
        }
    }

    fn rename(&mut self, mut log: Log) -> Log {
        self.rows_processed += 1;
        for (from, to) in &self.renames {
            let _renamed = rename_field(&mut log, from, to);
        }
        log
    }
}

impl Drop for RenameIter {
    fn drop(&mut self) {
        METRICS
            .workflow_step_rows
            .with_label_values(&[STEP_RENAME])
            .inc_by(self.rows_processed);
    }
}

impl Iterator for RenameIter {
    type Item = LogItem;

    fn next(&mut self) -> Option<Self::Item> {
        Some(match try_next_with_partial_stream!(self.input)? {
            PartialStreamItem::Log(log) => LogItem::Log(self.rename(log)),
            PartialStreamItem::PartialStreamLog(log, key) => {
                LogItem::PartialStreamLog(self.rename(log), key)
            }
            PartialStreamItem::PartialStreamDone(key) => LogItem::PartialStreamDone(key),
            PartialStreamItem::SourceDone(id) => LogItem::SourceDone(id),
        })
    }
}
