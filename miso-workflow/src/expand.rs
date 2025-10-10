use std::{collections::BTreeMap, vec};

use miso_workflow_types::{
    expand::{Expand, ExpandKind},
    field::Field,
    log::{Log, LogItem, LogIter},
    value::Value,
};

use crate::{
    interpreter::{extract_field, insert_field_value},
    log_utils::PartialStreamItem,
    try_next_with_partial_stream,
};

struct OutputIter {
    log: Log,
    fields: Vec<Field>,
    values: Vec<vec::IntoIter<Value>>,
    done: bool,
}

impl OutputIter {
    fn new(log: Log, fields: Vec<Field>, values: Vec<Vec<Value>>) -> Self {
        assert_eq!(fields.len(), values.len());

        Self {
            log,
            fields,
            values: values.into_iter().map(|x| x.into_iter()).collect(),
            done: false,
        }
    }
}

impl Iterator for OutputIter {
    type Item = Log;

    fn next(&mut self) -> Option<Self::Item> {
        if self.done {
            return None;
        }

        if self.fields.is_empty() {
            self.done = true;
            return Some(std::mem::take(&mut self.log));
        }

        let mut log = self.log.clone();
        let mut is_done = true;

        for (field, values) in self.fields.iter().zip(self.values.iter_mut()) {
            let value = if let Some(value) = values.next() {
                is_done = false;
                value
            } else {
                Value::Null
            };

            insert_field_value(&mut log, field, value);
        }

        self.done = is_done;
        (!self.done).then_some(log)
    }
}

enum OutputState {
    None,
    Regular(OutputIter),
    Partial(OutputIter, usize),
}

pub struct ExpandIter {
    input: LogIter,
    config: Expand,
    state: OutputState,
}

impl ExpandIter {
    pub fn new(input: LogIter, config: Expand) -> Self {
        Self {
            input,
            config,
            state: OutputState::None,
        }
    }

    fn log_to_output_iter(&self, mut log: Log) -> OutputIter {
        let mut output_fields = Vec::with_capacity(self.config.fields.len());
        let mut output_values = Vec::with_capacity(self.config.fields.len());

        for field in &self.config.fields {
            match extract_field(&mut log, field) {
                Some(Value::Array(arr)) => {
                    output_fields.push(field.clone());
                    output_values.push(arr);
                }
                Some(Value::Object(obj)) => {
                    let expanded: Vec<Value> = match self.config.kind {
                        ExpandKind::Array => {
                            let mut arr = Vec::with_capacity(obj.len() * 2);
                            for (k, v) in obj {
                                arr.push(Value::String(k));
                                arr.push(v);
                            }
                            arr
                        }
                        ExpandKind::Bag => obj
                            .into_iter()
                            .map(|(k, v)| {
                                let mut record = BTreeMap::new();
                                record.insert(k, v);
                                Value::Object(record)
                            })
                            .collect(),
                    };

                    output_fields.push(field.clone());
                    output_values.push(expanded);
                }
                Some(other) => {
                    // Get it back in.
                    // We optimize on there being an object / array of the field
                    // the user has provided, so there's no need to clone the object / array.
                    insert_field_value(&mut log, field, other);
                }
                _ => {}
            }
        }

        OutputIter::new(log, output_fields, output_values)
    }
}

impl Iterator for ExpandIter {
    type Item = LogItem;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            self.state = match &mut self.state {
                OutputState::None => match try_next_with_partial_stream!(self.input)? {
                    PartialStreamItem::Log(log) => {
                        OutputState::Regular(self.log_to_output_iter(log))
                    }
                    PartialStreamItem::PartialStreamLog(log, id) => {
                        OutputState::Partial(self.log_to_output_iter(log), id)
                    }
                    PartialStreamItem::PartialStreamDone(id) => {
                        return Some(LogItem::PartialStreamDone(id));
                    }
                },
                OutputState::Regular(iter) => {
                    if let Some(log) = iter.next() {
                        return Some(LogItem::Log(log));
                    }
                    OutputState::None
                }
                OutputState::Partial(iter, id) => {
                    if let Some(log) = iter.next() {
                        return Some(LogItem::PartialStreamLog(log, *id));
                    }
                    OutputState::None
                }
            }
        }
    }
}
