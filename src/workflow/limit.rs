use hashbrown::HashMap;

use crate::{
    log::{LogItem, LogIter},
    try_next,
};

use super::partial_stream::get_partial_id;

pub struct LimitIter {
    input: LogIter,
    limit: u32,
    streamed: u32,
    partial_limits: HashMap<usize, Option<u32>>,
}

impl LimitIter {
    pub fn new(input: LogIter, limit: u32) -> Self {
        Self {
            input,
            limit,
            streamed: 0,
            partial_limits: HashMap::new(),
        }
    }
}

impl Iterator for LimitIter {
    type Item = LogItem;

    fn next(&mut self) -> Option<Self::Item> {
        if self.streamed == self.limit {
            return None;
        }

        while let Some(log) = try_next!(self.input) {
            let should_stream = match get_partial_id(&log) {
                None => {
                    self.streamed += 1;
                    true
                }
                Some((id, true)) => {
                    self.partial_limits.remove(&id);
                    true
                }
                Some((id, false)) => match self.partial_limits.entry(id).or_insert(Some(0)) {
                    Some(streamed) if *streamed == self.limit => false,
                    Some(streamed) => {
                        *streamed += 1;
                        true
                    }
                    None => false,
                },
            };

            if should_stream {
                return Some(LogItem::Log(log));
            }
        }

        None
    }
}
