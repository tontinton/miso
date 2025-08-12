use miso_workflow_types::{
    log::{Log, LogItem, LogIter},
    project::ProjectField,
};
use tracing::warn;

use crate::{
    interpreter::{LogInterpreter, Val, insert_field_value},
    log_utils::PartialStreamItem,
    try_next_with_partial_stream,
};

pub struct ProjectIter {
    input: LogIter,
    project_fields: Vec<ProjectField>,
    extend: bool,
}

impl ProjectIter {
    pub fn new_project(input: LogIter, project_fields: Vec<ProjectField>) -> Self {
        Self {
            input,
            project_fields,
            extend: false,
        }
    }

    pub fn new_extend(input: LogIter, project_fields: Vec<ProjectField>) -> Self {
        Self {
            input,
            project_fields,
            extend: true,
        }
    }

    fn eval(&self, mut log: Log) -> Log {
        let mut output = Log::new();

        {
            let interpreter = LogInterpreter { log: &log };

            for field in &self.project_fields {
                match interpreter.eval(&field.from) {
                    Ok(Val(None)) => {} // Skip.
                    Ok(v) => {
                        let owned = v.0.unwrap().into_owned();
                        insert_field_value(&mut output, &field.to, owned);
                    }
                    Err(e) => {
                        warn!("Project failed: {e}");
                        continue;
                    }
                };
            }
        }

        if self.extend {
            log.extend(output);
        } else {
            log = output;
        }

        log
    }
}

impl Iterator for ProjectIter {
    type Item = LogItem;

    fn next(&mut self) -> Option<Self::Item> {
        Some(match try_next_with_partial_stream!(self.input)? {
            PartialStreamItem::Log(log) => LogItem::Log(self.eval(log)),
            PartialStreamItem::PartialStreamLog(log, id) => {
                LogItem::PartialStreamLog(self.eval(log), id)
            }
            PartialStreamItem::PartialStreamDone(id) => LogItem::PartialStreamDone(id),
        })
    }
}
