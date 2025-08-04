use miso_workflow_types::{
    log::{Log, LogItem, LogIter},
    project::ProjectField,
};
use tracing::warn;

use crate::interpreter::insert_field_value;

use super::{
    interpreter::{LogInterpreter, Val},
    try_next_with_partial_passthrough,
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
}

impl Iterator for ProjectIter {
    type Item = LogItem;

    fn next(&mut self) -> Option<Self::Item> {
        let mut log = try_next_with_partial_passthrough!(self.input)?;
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

        Some(LogItem::Log(log))
    }
}
