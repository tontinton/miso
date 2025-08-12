use miso_workflow_types::{
    expr::Expr,
    log::{Log, LogItem, LogIter},
};
use tracing::warn;

use crate::log_utils::PartialStreamItem;

use super::{interpreter::LogInterpreter, try_next_with_partial_stream};

pub struct FilterIter {
    input: LogIter,
    expr: Expr,
}

impl FilterIter {
    pub fn new(input: LogIter, expr: Expr) -> Self {
        Self { input, expr }
    }

    fn keep(&self, log: &Log) -> bool {
        let interpreter = LogInterpreter { log };
        match interpreter.eval(&self.expr) {
            Ok(v) => v.to_bool(),
            Err(e) => {
                warn!("Filter failed: {e}");
                false
            }
        }
    }
}

impl Iterator for FilterIter {
    type Item = LogItem;

    fn next(&mut self) -> Option<Self::Item> {
        while let Some(item) = try_next_with_partial_stream!(self.input) {
            let log_item_to_stream = match item {
                PartialStreamItem::Log(log) => self.keep(&log).then_some(LogItem::Log(log)),
                PartialStreamItem::PartialStreamLog(log, id) => self
                    .keep(&log)
                    .then_some(LogItem::PartialStreamLog(log, id)),
                PartialStreamItem::PartialStreamDone(id) => Some(LogItem::PartialStreamDone(id)),
            };

            if let Some(log_item) = log_item_to_stream {
                return Some(log_item);
            }
        }

        None
    }
}
